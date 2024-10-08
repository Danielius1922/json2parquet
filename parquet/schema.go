package parquet

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	tfJson "github.com/thermofisher/json2parquet/json"
	"github.com/thermofisher/json2parquet/log"
)

type SchemaBuilder struct {
	fields map[string]Node

	firstRun       bool
	requiredFields map[string]struct{}
}

var (
	ErrTypeNotSupported = errors.New("type not supported")
	ErrTypeMismatch     = errors.New("type mismatch")
)

func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		fields: make(map[string]Node),

		firstRun:       true, // after first update run the default repetition should be optional
		requiredFields: make(map[string]struct{}),
	}
}

type inferedTypeAction int

const (
	inferedTypeActionNone inferedTypeAction = iota
	inferedTypeActionUpgrade
	inferedTypeActionMismatch
)

func checkOrUpdateArrayElementInferedType(field, newField Node) inferedTypeAction {
	f1ListElement := field.(*ListNode).Element()
	// type is set to empty array without type -> can be upgraded to whatever type
	if f1ListElement.GetType() == NodeTypeNone && f1ListElement.GetLogicalType() == LogicalTypeNone {
		return inferedTypeActionUpgrade
	}
	// new empty array -> can be accepted by whatever array already set
	f2ListElement := newField.(*ListNode).Element()
	if f2ListElement.GetType() == NodeTypeNone && f2ListElement.GetLogicalType() == LogicalTypeNone {
		return inferedTypeActionNone
	}
	return checkOrUpdateInferedType(f1ListElement, f2ListElement)
}

func checkOrUpdateInferedType(field, newField Node) inferedTypeAction {
	f1Type := field.GetType()
	f2Type := newField.GetType()
	// allow change of inferred type from int64 to float64
	if f1Type == NodeTypeInt64 && f2Type == NodeTypeFloat64 {
		return inferedTypeActionUpgrade
	}
	// a valid integer value is also a valid float value
	if f1Type == NodeTypeFloat64 && f2Type == NodeTypeInt64 {
		return inferedTypeActionNone
	}
	f1LType := field.GetLogicalType()
	if f1Type == NodeTypeByteArray && f2Type == NodeTypeByteArray {
		// a valid base64 string is also a valid string
		if f1LType == LogicalTypeUTF8 {
			return inferedTypeActionNone
		}
		// allow change from byte array to string
		return inferedTypeActionUpgrade
	}
	f2LType := newField.GetLogicalType()
	if f1Type == NodeTypeNone && f2Type == NodeTypeNone {
		if f1LType == LogicalTypeList && f2LType == LogicalTypeList {
			return checkOrUpdateArrayElementInferedType(field, newField)
		}
	}
	return inferedTypeActionMismatch
}

func (sb *SchemaBuilder) checkOrUpdateNode(key string, parsedNode Node) (Node, error) {
	field, ok := sb.fields[key]
	if !ok {
		sb.fields[key] = parsedNode
		return parsedNode, nil
	}
	if !field.IsEqual(parsedNode) {
		action := checkOrUpdateInferedType(field, parsedNode)
		if action == inferedTypeActionUpgrade {
			log.Logger().Debugf("changed inferred field %v to %v", field.Print(), parsedNode.Print())
			parsedNode.SetRepetition(field.GetRepetition())
			sb.fields[key] = parsedNode
			return parsedNode, nil
		}
		if action == inferedTypeActionNone {
			log.Logger().Debugf("parsed field %v accepted by previously inferred %v", parsedNode.Print(), field.Print())
			return field, nil
		}
		return nil, fmt.Errorf("%w: field(%v) does not match expected field(%v) ", ErrTypeMismatch, parsedNode.Print(),
			field.Print())
	}
	return field, nil
}

func getNodeType(key string, value interface{}) (NodeType, LogicalType, error) {
	switch reflect.TypeOf(value).Kind() {
	case reflect.Bool:
		return NodeTypeBoolean, LogicalTypeNone, nil
	case reflect.String:
		if number, ok := value.(json.Number); ok {
			_, err := number.Int64()
			if err == nil {
				return NodeTypeInt64, LogicalTypeNone, nil
			}
			_, err = number.Float64()
			if err == nil {
				return NodeTypeFloat64, LogicalTypeNone, nil
			}
			return NodeTypeNone, LogicalTypeNone, fmt.Errorf("%w: unrecognized number type(%v:%T)", ErrTypeNotSupported, key, value)
		}
		_, ok := tfJson.ToByteString(value)
		if ok {
			return NodeTypeByteArray, LogicalTypeNone, nil
		}
		return NodeTypeByteArray, LogicalTypeUTF8, nil
	case reflect.Slice:
		return NodeTypeNone, LogicalTypeList, nil
	}
	return NodeTypeNone, LogicalTypeNone, fmt.Errorf("%w: unrecognized type(%v:%T)", ErrTypeNotSupported, key, value)
}

func inferArrayElementNode(slice []interface{}) (Node, error) {
	if len(slice) == 0 {
		return NewTemporaryNode("element", parquet.Repetitions.Repeated), nil
	}
	var arrayNode Node
	for i, e := range slice {
		node, err := getNode("element", e, parquet.Repetitions.Repeated)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			arrayNode = node
			continue
		}
		if arrayNode.IsEqual(node) {
			continue
		}

		action := checkOrUpdateInferedType(arrayNode, node)
		if action == inferedTypeActionUpgrade {
			arrayNode = node
			continue
		}
		if action == inferedTypeActionNone {
			continue
		}
		return nil, fmt.Errorf("%w: array field(%v) does not match expected array field(%v) ", ErrTypeMismatch, node.Print(),
			arrayNode.Print())
	}
	return arrayNode, nil
}

func newArrayNode(key string, slice []interface{}, repetition parquet.Repetition) (Node, error) {
	element, err := inferArrayElementNode(slice)
	if err != nil {
		return nil, err
	}
	return NewListNode(key, repetition, element), nil
}

func getNodeByType(key string, nodeType NodeType, logicalType LogicalType, repetition parquet.Repetition, value interface{}) (Node, error) {
	switch logicalType {
	case LogicalTypeUTF8:
		if nodeType == NodeTypeByteArray {
			return NewByteArrayNode(key, repetition, LogicalTypeUTF8), nil
		}
		return nil, fmt.Errorf("invalid physical type(%v) for logical type(%v)", nodeType, logicalType)
	case LogicalTypeList:
		if nodeType == NodeTypeNone {
			return newArrayNode(key, value.([]interface{}), repetition)
		}
		return nil, fmt.Errorf("invalid physical type(%v) for logical type(%v)", nodeType, logicalType)
	case LogicalTypeNone:
	default:
		return nil, fmt.Errorf("invalid physical type(%v) for logical type(%v)", nodeType, logicalType)
	}

	// LogicalTypeNone
	switch nodeType {
	case NodeTypeBoolean:
		return NewBooleanNode(key, repetition), nil
	case NodeTypeInt64:
		return NewInt64Node(key, repetition), nil
	case NodeTypeFloat64:
		return NewFloat64Node(key, repetition), nil
	case NodeTypeByteArray:
		return NewByteArrayNode(key, repetition, LogicalTypeNone), nil
	}
	return nil, fmt.Errorf("invalid physical type(%v) for logical type(%v)", nodeType, logicalType)
}

func getNode(key string, value interface{}, repetition parquet.Repetition) (Node, error) {
	nodeType, logicalType, err := getNodeType(key, value)
	if err != nil {
		return nil, err
	}
	return getNodeByType(key, nodeType, logicalType, repetition, value)
}

func (sb *SchemaBuilder) updateField(key string, value interface{}, repetition parquet.Repetition) (Node, error) {
	parsedNode, err := getNode(key, value, repetition)
	if err != nil {
		return nil, err
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
		pqNode, err := node.Node()
		if err != nil {
			return nil, err
		}
		fields = append(fields, pqNode)
	}
	return schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
}

func (sb *SchemaBuilder) Schema() (*schema.Schema, error) {
	root, err := sb.root()
	if err != nil {
		return nil, err
	}
	return schema.NewSchema(root), nil
}
