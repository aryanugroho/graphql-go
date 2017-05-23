package graphql

import (
	"context"
	"fmt"
	"reflect"

	"encoding/json"

	"strconv"

	"github.com/neelance/graphql-go/errors"
	"github.com/neelance/graphql-go/internal/common"
	"github.com/neelance/graphql-go/internal/exec"
	"github.com/neelance/graphql-go/internal/exec/packer"
	"github.com/neelance/graphql-go/internal/exec/resolvable"
	"github.com/neelance/graphql-go/internal/exec/selected"
	"github.com/neelance/graphql-go/internal/query"
	"github.com/neelance/graphql-go/internal/schema"
	"github.com/neelance/graphql-go/internal/validation"
	"github.com/neelance/graphql-go/introspection"
	"github.com/neelance/graphql-go/log"
	"github.com/neelance/graphql-go/trace"
)

// ID represents GraphQL's "ID" type. A custom type may be used instead.
type ID string

func (_ ID) ImplementsGraphQLType(name string) bool {
	return name == "ID"
}

func (id *ID) UnmarshalGraphQL(input interface{}) error {
	switch input := input.(type) {
	case string:
		*id = ID(input)
		return nil
	default:
		return fmt.Errorf("wrong type")
	}
}

func (id ID) MarshalJSON() ([]byte, error) {
	return strconv.AppendQuote(nil, string(id)), nil
}

func ParseSchema(schemaString string) *SchemaBuilder {
	b, err := TryParseSchema(schemaString)
	if err != nil {
		panic(err)
	}
	return b
}

func TryParseSchema(schemaString string) (*SchemaBuilder, error) {
	b := &SchemaBuilder{
		schema:        schema.New(),
		resolvers:     make(resolvable.TypeToResolversMap),
		packerBuilder: packer.NewBuilder(),
	}
	if err := b.schema.Parse(schemaString); err != nil {
		return nil, err
	}
	return b, nil
}

type SchemaBuilder struct {
	schema        *schema.Schema
	resolvers     resolvable.TypeToResolversMap
	packerBuilder *packer.Builder
}

type untypedResolver func(valueType reflect.Type, args common.InputValueList) (*resolvable.Resolver, error)

func (b *SchemaBuilder) Resolvers(graphqlType string, goType interface{}, fieldMap map[string]interface{}) {
	st, ok := b.schema.Types[graphqlType]
	if !ok {
		panic("type not found") // TODO
	}

	t := common.TypePair{GraphQLType: st, GoType: reflect.TypeOf(goType)}
	m := make(resolvable.FieldToResolverMap)
	for field, resolver := range fieldMap {

		f := fields(t.GraphQLType).Get(field)
		if f == nil {
			panic("field not found") // TODO
		}

		var utRes untypedResolver
		switch r := resolver.(type) {
		case string:
			utRes = b.newResolverField(r)
		default:
			utRes = b.newResolverFunc(reflect.ValueOf(r))
		}

		res, err := utRes(t.GoType, f.Args)
		if err != nil {
			panic(err) // TODO
		}

		m[field] = res
	}
	b.resolvers[t] = m
}

func (b *SchemaBuilder) newResolverField(goField string) untypedResolver {
	return func(valueType reflect.Type, args common.InputValueList) (*resolvable.Resolver, error) {
		if valueType.Kind() == reflect.Ptr {
			valueType = valueType.Elem()
		}
		sf, ok := valueType.FieldByName(goField)
		if !ok {
			return nil, fmt.Errorf("type %s has no field %q", valueType, goField)
		}

		return &resolvable.Resolver{
			Select: func(args map[string]interface{}) (resolvable.SelectedResolver, bool, error) {
				return func(ctx context.Context, parent reflect.Value) (reflect.Value, error) {
					if parent.Kind() == reflect.Ptr {
						parent = parent.Elem()
					}
					return parent.FieldByIndex(sf.Index), nil
				}, false, nil
			},
			ResultType: sf.Type,
		}, nil
	}
}

var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
var errorType = reflect.TypeOf((*error)(nil)).Elem()

func (b *SchemaBuilder) newResolverFunc(fn reflect.Value) untypedResolver {
	return func(valueType reflect.Type, args common.InputValueList) (*resolvable.Resolver, error) {
		in := make([]reflect.Type, fn.Type().NumIn())
		for i := range in {
			in[i] = fn.Type().In(i)
		}
		in = in[1:] // first parameter is value

		hasContext := len(in) > 0 && in[0] == contextType
		if hasContext {
			in = in[1:]
		}

		var argsPacker *packer.StructPacker
		if len(args) > 0 {
			if len(in) == 0 {
				return nil, fmt.Errorf("must have parameter for field arguments")
			}
			var err error
			argsPacker, err = b.packerBuilder.MakeStructPacker(args, in[0])
			if err != nil {
				return nil, err
			}
			in = in[1:]
		}

		if len(in) > 0 {
			return nil, fmt.Errorf("too many parameters")
		}

		if fn.Type().NumOut() > 2 {
			return nil, fmt.Errorf("too many return values")
		}

		hasError := fn.Type().NumOut() == 2
		if hasError {
			if fn.Type().Out(1) != errorType {
				return nil, fmt.Errorf(`must have "error" as its second return value`)
			}
		}

		return &resolvable.Resolver{
			Select: func(args map[string]interface{}) (resolvable.SelectedResolver, bool, error) {
				var packedArgs reflect.Value
				if argsPacker != nil {
					var err error
					packedArgs, err = argsPacker.Pack(args)
					if err != nil {
						return nil, false, err
					}
				}

				async := hasContext || argsPacker != nil || hasError
				return func(ctx context.Context, parent reflect.Value) (reflect.Value, error) {
					in := []reflect.Value{parent}
					if hasContext {
						in = append(in, reflect.ValueOf(ctx))
					}
					if argsPacker != nil {
						in = append(in, packedArgs)
					}
					callOut := fn.Call(in)
					if hasError && !callOut[1].IsNil() {
						return reflect.Value{}, callOut[1].Interface().(error)
					}
					return callOut[0], nil
				}, async, nil
			},
			ResultType: fn.Type().Out(0),
		}, nil
	}
}

// ParseSchema parses a GraphQL schema and attaches the given root resolver. It returns an error if
// the Go type signature of the resolvers does not match the schema. If nil is passed as the
// resolver, then the schema can not be executed, but it may be inspected (e.g. with ToJSON).
func (b *SchemaBuilder) Build(root interface{}, opts ...SchemaOpt) *Schema {
	s, err := b.TryBuild(root, opts...)
	if err != nil {
		panic(err)
	}
	return s
}

func (b *SchemaBuilder) TryBuild(root interface{}, opts ...SchemaOpt) (*Schema, error) {
	s := &Schema{
		schema:         b.schema,
		maxParallelism: 10,
		tracer:         trace.OpenTracingTracer{},
		logger:         &log.DefaultLogger{},
	}
	for _, opt := range opts {
		opt(s)
	}

	if root != nil {
		r, err := resolvable.ApplyResolvers(s.schema, b.resolvers, root)
		if err != nil {
			return nil, err
		}
		s.res = r
	}

	b.packerBuilder.Finish()

	return s, nil
}

// Schema represents a GraphQL schema with an optional resolver.
type Schema struct {
	schema *schema.Schema
	res    *resolvable.Schema

	maxParallelism int
	tracer         trace.Tracer
	logger         log.Logger
}

// SchemaOpt is an option to pass to ParseSchema or MustParseSchema.
type SchemaOpt func(*Schema)

// MaxParallelism specifies the maximum number of resolvers per request allowed to run in parallel. The default is 10.
func MaxParallelism(n int) SchemaOpt {
	return func(s *Schema) {
		s.maxParallelism = n
	}
}

// Tracer is used to trace queries and fields. It defaults to trace.OpenTracingTracer.
func Tracer(tracer trace.Tracer) SchemaOpt {
	return func(s *Schema) {
		s.tracer = tracer
	}
}

// Logger is used to log panics durring query execution. It defaults to exec.DefaultLogger.
func Logger(logger log.Logger) SchemaOpt {
	return func(s *Schema) {
		s.logger = logger
	}
}

// Response represents a typical response of a GraphQL server. It may be encoded to JSON directly or
// it may be further processed to a custom response type, for example to include custom error data.
type Response struct {
	Data       json.RawMessage        `json:"data,omitempty"`
	Errors     []*errors.QueryError   `json:"errors,omitempty"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
}

// Exec executes the given query with the schema's resolver. It panics if the schema was created
// without a resolver. If the context get cancelled, no further resolvers will be called and a
// the context error will be returned as soon as possible (not immediately).
func (s *Schema) Exec(ctx context.Context, queryString string, operationName string, variables map[string]interface{}) *Response {
	if s.res == nil {
		panic("schema created without resolver, can not exec")
	}
	return s.exec(ctx, queryString, operationName, variables, s.res)
}

func (s *Schema) exec(ctx context.Context, queryString string, operationName string, variables map[string]interface{}, res *resolvable.Schema) *Response {
	doc, qErr := query.Parse(queryString)
	if qErr != nil {
		return &Response{Errors: []*errors.QueryError{qErr}}
	}

	errs := validation.Validate(s.schema, doc)
	if len(errs) != 0 {
		return &Response{Errors: errs}
	}

	op, err := getOperation(doc, operationName)
	if err != nil {
		return &Response{Errors: []*errors.QueryError{errors.Errorf("%s", err)}}
	}

	r := &exec.Request{
		Request: selected.Request{
			Doc:    doc,
			Vars:   variables,
			Schema: s.schema,
		},
		Limiter: make(chan struct{}, s.maxParallelism),
		Tracer:  s.tracer,
		Logger:  s.logger,
	}
	varTypes := make(map[string]*introspection.Type)
	for _, v := range op.Vars {
		t, err := common.ResolveType(v.Type, s.schema.Resolve)
		if err != nil {
			return &Response{Errors: []*errors.QueryError{err}}
		}
		varTypes[v.Name.Name] = introspection.WrapType(t)
	}
	traceCtx, finish := s.tracer.TraceQuery(ctx, queryString, operationName, variables, varTypes)
	data, errs := r.Execute(traceCtx, res, op)
	finish(errs)

	return &Response{
		Data:   data,
		Errors: errs,
	}
}

func getOperation(document *query.Document, operationName string) (*query.Operation, error) {
	if len(document.Operations) == 0 {
		return nil, fmt.Errorf("no operations in query document")
	}

	if operationName == "" {
		if len(document.Operations) > 1 {
			return nil, fmt.Errorf("more than one operation in query document and no operation name given")
		}
		for _, op := range document.Operations {
			return op, nil // return the one and only operation
		}
	}

	op := document.Operations.Get(operationName)
	if op == nil {
		return nil, fmt.Errorf("no operation with name %q", operationName)
	}
	return op, nil
}

func fields(t common.Type) schema.FieldList {
	switch t := t.(type) {
	case *schema.Object:
		return t.Fields
	case *schema.Interface:
		return t.Fields
	default:
		return nil
	}
}
