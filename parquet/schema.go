package parquet

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sort"

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

func checkOrUpdateArrayElementInferedType(field, newField Node) (inferedTypeAction, Node) {
	f1ListElement := field.(*ListNode).Element()
	// type is set to empty array without type -> can be upgraded to whatever type
	if f1ListElement.GetType() == NodeTypeNone && f1ListElement.GetLogicalType() == LogicalTypeNone {
		return inferedTypeActionUpgrade, newField
	}
	// new empty array -> can be accepted by whatever array already set
	f2ListElement := newField.(*ListNode).Element()
	if f2ListElement.GetType() == NodeTypeNone && f2ListElement.GetLogicalType() == LogicalTypeNone {
		return inferedTypeActionNone, nil
	}
	ita, node := checkOrUpdateInferedType(f1ListElement, f2ListElement)
	if ita != inferedTypeActionUpgrade {
		return ita, nil
	}
	newField.(*ListNode).SetElement(node)
	return inferedTypeActionUpgrade, newField
}

func checkOrUpdateInferedType(field, newField Node) (inferedTypeAction, Node) { //nolint:gocyclo
	f1Type := field.GetType()
	f2Type := newField.GetType()
	// allow change of inferred type from int64 to float64
	if f1Type == NodeTypeInt64 && f2Type == NodeTypeFloat64 {
		return inferedTypeActionUpgrade, newField
	}
	// a valid integer value is also a valid float value
	if f1Type == NodeTypeFloat64 && f2Type == NodeTypeInt64 {
		return inferedTypeActionNone, nil
	}
	f1LType := field.GetLogicalType()
	if f1Type == NodeTypeByteArray && f2Type == NodeTypeByteArray {
		f1EType := field.GetExtendedType()
		f2EType := newField.GetExtendedType()
		if f1EType == ExtendedTypeNone && f2EType == ExtendedTypeNone {
			// a valid base64 string is also a valid string
			if f1LType == LogicalTypeUTF8 {
				return inferedTypeActionNone, nil
			}
			// allow change from byte array to string
			return inferedTypeActionUpgrade, newField
		}
		if f1EType == ExtendedTypeRFC3339 || f2EType == ExtendedTypeRFC3339 {
			// we have a different string type -> the common type is always a UTF8 string
			return inferedTypeActionUpgrade, NewByteArrayNode(field.GetName(), field.GetRepetition(), LogicalTypeUTF8, ExtendedTypeNone)
		}
	}
	f2LType := newField.GetLogicalType()
	if f1Type == NodeTypeNone && f2Type == NodeTypeNone {
		if f1LType == LogicalTypeList && f2LType == LogicalTypeList {
			return checkOrUpdateArrayElementInferedType(field, newField)
		}
	}
	return inferedTypeActionMismatch, nil
}

func (sb *SchemaBuilder) checkOrUpdateNode(key string, parsedNode Node) (Node, error) {
	field, ok := sb.fields[key]
	if !ok {
		sb.fields[key] = parsedNode
		return parsedNode, nil
	}
	if !field.IsEqual(parsedNode) {
		action, updatedField := checkOrUpdateInferedType(field, parsedNode)
		if action == inferedTypeActionUpgrade {
			log.Logger().Debugf("changed inferred field %v to %v", field.Print(), updatedField.Print())
			updatedField.SetRepetition(field.GetRepetition())
			sb.fields[key] = updatedField
			return updatedField, nil
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

func getNodeType(key string, value interface{}) (NodeType, LogicalType, ExtendedType, error) {
	switch reflect.TypeOf(value).Kind() {
	case reflect.Bool:
		return NodeTypeBoolean, LogicalTypeNone, ExtendedTypeNone, nil
	case reflect.String:
		if number, ok := value.(json.Number); ok {
			_, err := number.Int64()
			if err == nil {
				return NodeTypeInt64, LogicalTypeNone, ExtendedTypeNone, nil
			}
			_, err = number.Float64()
			if err == nil {
				return NodeTypeFloat64, LogicalTypeNone, ExtendedTypeNone, nil
			}
			return NodeTypeNone, LogicalTypeNone, ExtendedTypeNone, fmt.Errorf("%w: unrecognized number type(%v:%T)", ErrTypeNotSupported, key, value)
		}
		str := value.(string)
		if tfJson.IsRFC3339(str) {
			return NodeTypeByteArray, LogicalTypeNone, ExtendedTypeRFC3339, nil
		}
		if tfJson.IsBase64Encoded(str) {
			return NodeTypeByteArray, LogicalTypeNone, ExtendedTypeNone, nil
		}
		return NodeTypeByteArray, LogicalTypeUTF8, ExtendedTypeNone, nil
	case reflect.Slice:
		return NodeTypeNone, LogicalTypeList, ExtendedTypeNone, nil
	}
	return NodeTypeNone, LogicalTypeNone, ExtendedTypeNone, fmt.Errorf("%w: unrecognized type(%v:%T)", ErrTypeNotSupported, key, value)
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

		action, updatedNode := checkOrUpdateInferedType(arrayNode, node)
		if action == inferedTypeActionUpgrade {
			arrayNode = updatedNode
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

func getNodeByType(key string, nodeType NodeType, logicalType LogicalType, extendedType ExtendedType, repetition parquet.Repetition, value interface{}) (Node, error) {
	if extendedType == ExtendedTypeRFC3339 {
		if nodeType == NodeTypeByteArray {
			return NewByteArrayNode(key, repetition, LogicalTypeNone, ExtendedTypeRFC3339), nil
		}
		return nil, fmt.Errorf("invalid physical type(%v) for extended type(%v)", nodeType, extendedType)
	}
	// ExtendedTypeNone

	switch logicalType {
	case LogicalTypeUTF8:
		if nodeType == NodeTypeByteArray {
			return NewByteArrayNode(key, repetition, LogicalTypeUTF8, ExtendedTypeNone), nil
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
		return NewByteArrayNode(key, repetition, LogicalTypeNone, ExtendedTypeNone), nil
	}
	return nil, fmt.Errorf("invalid physical type(%v) for logical type(%v)", nodeType, logicalType)
}

func getNode(key string, value interface{}, repetition parquet.Repetition) (Node, error) {
	nodeType, logicalType, extendedType, err := getNodeType(key, value)
	if err != nil {
		return nil, err
	}
	return getNodeByType(key, nodeType, logicalType, extendedType, repetition, value)
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

func (sb *SchemaBuilder) Schema() *Schema {
	return &Schema{
		fields: maps.Clone(sb.fields),
	}
}

type Schema struct {
	fields map[string]Node
}

func (s *Schema) root() (*schema.GroupNode, error) {
	fields := make(schema.FieldList, 0, len(s.fields))
	for _, node := range s.fields {
		pqNode, err := node.Node()
		if err != nil {
			return nil, err
		}
		fields = append(fields, pqNode)
	}
	// sort fields by name to get consistent order for tests
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name() < fields[j].Name()
	})
	return schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
}

func (s *Schema) Schema() (*schema.Schema, error) {
	root, err := s.root()
	if err != nil {
		return nil, err
	}
	return schema.NewSchema(root), nil
}

func (s *Schema) FieldByPath(path []string) Node {
	if path == nil {
		return nil
	}
	name := path[0]
	remainder := path[1:]
	for _, f := range s.fields {
		if f.GetName() == name {
			return f.FieldByPath(remainder)
		}
	}
	return nil
}
