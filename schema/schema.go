package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	tfJson "github.com/thermofisher/json"
)

type SchemaNode interface {
	GetName() string
	GetType() NodeType

	GetRepetition() parquet.Repetition
	SetRepetition(repetition parquet.Repetition)

	Node() schema.Node
}

type SchemaBuilder struct {
	fields map[string]SchemaNode

	firstRun       bool
	requiredFields map[string]struct{}
}

var (
	ErrTypeNotSupported = errors.New("type not supported")
	ErrTypeMismatch     = errors.New("type mismatch")
)

func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		fields: make(map[string]SchemaNode),

		firstRun:       true, // after first update run the default repetion should be optional
		requiredFields: make(map[string]struct{}),
	}
}

func (sb *SchemaBuilder) checkOrUpdateNode(key string, parsedNode SchemaNode) (SchemaNode, error) {
	field, ok := sb.fields[key]
	if !ok {
		sb.fields[key] = parsedNode
		return parsedNode, nil
	}
	if field.GetType() != parsedNode.GetType() {
		// allow change of infered type from int64 to float64
		if field.GetType() == NodeTypeInt64 && parsedNode.GetType() == NodeTypeFloat64 {
			parsedNode.SetRepetition(field.GetRepetition())
			sb.fields[key] = parsedNode
			return parsedNode, nil
		}
		// a valid integer value is also a valid float value
		if field.GetType() == NodeTypeFloat64 && parsedNode.GetType() == NodeTypeInt64 {
			return field, nil
		}
		return nil, fmt.Errorf("%w: type(%v) does not match expected type(%v) ", ErrTypeMismatch, parsedNode.GetType(), field.GetType())
	}
	return field, nil
}

func (sb *SchemaBuilder) updateField(key string, value interface{}, repetition parquet.Repetition) (SchemaNode, error) {
	var parsedNode SchemaNode
	switch reflect.TypeOf(value).Kind() {
	case reflect.Bool:
		parsedNode = NewBooleanNode(key, repetition)
	case reflect.TypeFor[json.Number]().Kind():
		if _, ok := tfJson.ToInt64(value); ok {
			parsedNode = NewInt64Node(key, repetition)
			break
		}
		if _, ok := tfJson.ToFloat64(value); ok {
			parsedNode = NewFloat64Node(key, repetition)
			break
		}
		return nil, fmt.Errorf("%w: unrecognized type(%T)", ErrTypeNotSupported, value)
	default:
		return nil, fmt.Errorf("%w: unrecognized type(%T)", ErrTypeNotSupported, value)
	}
	return sb.checkOrUpdateNode(key, parsedNode)
}

func (sb *SchemaBuilder) UpdateSchema(obj tfJson.NDJsonRecord) error {
	repetition := parquet.Repetitions.Required
	if !sb.firstRun {
		repetition = parquet.Repetitions.Optional
	}
	missingFields := make(map[string]struct{}, len(sb.requiredFields))
	for k := range sb.requiredFields {
		missingFields[k] = struct{}{}
	}
	defer func() {
		sb.firstRun = false
	}()

	for key, value := range obj {
		field, err := sb.updateField(key, value, repetition)
		if err != nil {
			return err
		}
		if sb.firstRun {
			sb.requiredFields[key] = struct{}{}
		}
		delete(missingFields, field.GetName())
	}

	for key := range missingFields {
		field, ok := sb.fields[key]
		if ok {
			field.SetRepetition(parquet.Repetitions.Optional)
			sb.fields[key] = field
			delete(sb.requiredFields, key)
		}
	}
	return nil
}

func (sb *SchemaBuilder) root() (*schema.GroupNode, error) {
	fields := make(schema.FieldList, 0, len(sb.fields))
	for _, node := range sb.fields {
		fields = append(fields, node.Node())
	}
	return schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, -1)
}

func (sb *SchemaBuilder) Schema() (*schema.Schema, error) {
	root, err := sb.root()
	if err != nil {
		return nil, err
	}
	return schema.NewSchema(root), nil
}
