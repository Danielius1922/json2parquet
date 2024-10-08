package parquet

import (
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/thermofisher/json2parquet/log"
	"golang.org/x/exp/maps"
)

type NodeType int

const (
	NodeTypeNone NodeType = iota
	NodeTypeBoolean
	NodeTypeInt64
	NodeTypeFloat64
	NodeTypeByteArray
)

func (nt NodeType) ToType() parquet.Type {
	switch nt {
	case NodeTypeBoolean:
		return parquet.Types.Boolean
	case NodeTypeInt64:
		return parquet.Types.Int64
	case NodeTypeFloat64:
		return parquet.Types.Double
	case NodeTypeByteArray:
		return parquet.Types.ByteArray
	}
	return parquet.Types.Undefined
}

func (nt NodeType) String() string {
	return nt.ToType().String()
}

type LogicalType int

const (
	LogicalTypeNone LogicalType = iota
	LogicalTypeUTF8
	LogicalTypeList
)

func (lt LogicalType) ToLogicalType() schema.LogicalType {
	switch lt {
	case LogicalTypeNone:
		return &schema.NoLogicalType{}
	case LogicalTypeUTF8:
		return &schema.StringLogicalType{}
	case LogicalTypeList:
		return &schema.ListLogicalType{}
	}
	return &schema.UnknownLogicalType{}
}

func (lt LogicalType) String() string {
	return lt.ToLogicalType().String()
}

var ErrOpNotSupported = errors.New("not supported")

type Node interface {
	GetName() string
	GetType() NodeType
	GetRepetition() parquet.Repetition
	SetRepetition(repetition parquet.Repetition)
	GetLogicalType() LogicalType

	Print() string
	Fields() ([]Node, error)
	IsEqual(Node) bool

	Node() (schema.Node, error)
}

type node struct {
	name        string
	typ         NodeType
	repetition  parquet.Repetition
	logicalType LogicalType
}

func (bn *node) GetName() string {
	return bn.name
}

func (bn *node) GetType() NodeType {
	return bn.typ
}

func (bn *node) GetRepetition() parquet.Repetition {
	return bn.repetition
}

func (bn *node) SetRepetition(repetition parquet.Repetition) {
	bn.repetition = repetition
}

func (bn *node) GetLogicalType() LogicalType {
	return bn.logicalType
}

func (bn *node) SetLogicalType(LogicalType) error {
	return ErrOpNotSupported
}

func (bn *node) Fields() ([]Node, error) {
	return nil, ErrOpNotSupported
}

func (bn *node) Print() string {
	return bn.name + ":" + bn.typ.String() + ":" + bn.logicalType.String()
}

func (bn *node) IsEqual(n Node) bool {
	return bn.GetType() == n.GetType() && bn.GetLogicalType() == n.GetLogicalType()
}

type TemporaryNode struct {
	node
}

func NewTemporaryNode(name string, repetition parquet.Repetition) *TemporaryNode {
	return &TemporaryNode{
		node{
			name:        name,
			typ:         NodeTypeNone,
			repetition:  repetition,
			logicalType: LogicalTypeNone,
		},
	}
}

func (tn *TemporaryNode) Node() (schema.Node, error) {
	return nil, ErrOpNotSupported
}

type BooleanNode struct {
	node
}

func NewBooleanNode(name string, repetition parquet.Repetition) *BooleanNode {
	return &BooleanNode{
		node{
			name:       name,
			typ:        NodeTypeBoolean,
			repetition: repetition,
		},
	}
}

func (bn *BooleanNode) Node() (schema.Node, error) {
	return schema.NewBooleanNode(bn.name, bn.repetition, -1), nil
}

type Int64Node struct {
	node
}

func NewInt64Node(name string, repetition parquet.Repetition) *Int64Node {
	return &Int64Node{
		node{
			name:       name,
			typ:        NodeTypeInt64,
			repetition: repetition,
		},
	}
}

func (bn *Int64Node) Node() (schema.Node, error) {
	return schema.NewInt64Node(bn.name, bn.repetition, -1), nil
}

type Float64Node struct {
	node
}

func NewFloat64Node(name string, repetition parquet.Repetition) *Float64Node {
	return &Float64Node{
		node{
			name:       name,
			typ:        NodeTypeFloat64,
			repetition: repetition,
		},
	}
}

func (bn *Float64Node) Node() (schema.Node, error) {
	return schema.NewFloat64Node(bn.name, bn.repetition, -1), nil
}

type ByteArrayNode struct {
	node
}

func NewByteArrayNode(name string, repetition parquet.Repetition, logicalType LogicalType) *ByteArrayNode {
	return &ByteArrayNode{
		node{
			name:        name,
			typ:         NodeTypeByteArray,
			repetition:  repetition,
			logicalType: logicalType,
		},
	}
}

func (bn *ByteArrayNode) Node() (schema.Node, error) {
	if bn.logicalType == LogicalTypeUTF8 {
		return schema.NewPrimitiveNodeLogical(bn.name, bn.repetition,
			schema.StringLogicalType{}, parquet.Types.ByteArray, 0, -1)
	}
	return schema.NewByteArrayNode(bn.name, bn.repetition, -1), nil
}

func (bn *ByteArrayNode) SetLogicalType(lt LogicalType) error {
	if lt == LogicalTypeNone || lt == LogicalTypeUTF8 {
		bn.logicalType = lt
		return nil
	}
	return fmt.Errorf("cannot set logical type(%v) for byte array", lt)
}

type GroupNode struct {
	node
	fields []Node
}

func toFields(nodes []Node) (schema.FieldList, error) {
	fields := make(schema.FieldList, 0, len(nodes))
	for _, n := range nodes {
		field, err := n.Node()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func NewGroupNode(name string, repetition parquet.Repetition, fields []Node, logicalType LogicalType) *GroupNode {
	return &GroupNode{
		node: node{
			name:        name,
			typ:         NodeTypeNone,
			repetition:  repetition,
			logicalType: logicalType,
		},
		fields: fields,
	}
}

func (gn *GroupNode) Fields() ([]Node, error) {
	return gn.fields, nil
}

func (gn *GroupNode) Node() (schema.Node, error) {
	fields, err := toFields(gn.fields)
	if err != nil {
		return nil, err
	}
	return schema.NewGroupNodeLogical(gn.name, gn.repetition, fields, gn.logicalType.ToLogicalType(), -1)
}

func (gn *GroupNode) matchFields(fields []Node) bool {
	f1map := make(map[string]Node, len(gn.fields))
	for _, f := range gn.fields {
		f1map[f.GetName()] = f
	}
	for _, f2 := range fields {
		f1, ok := f1map[f2.GetName()]
		if !ok {
			log.Logger().Debugf("fields mismatch: unexpected field(%v)", f2.GetName())
			return false
		}
		if !f1.IsEqual(f2) {
			log.Logger().Debugf("fields mismatch: types for field(%v) do not match (%v vs %v)",
				f1.GetName(), f1.Print(), f2.Print())
			return false
		}
		delete(f1map, f2.GetName())
	}
	if len(f1map) > 0 {
		log.Logger().Debugf("fields mismatch: missing fields(%v)", maps.Keys(f1map))
		return false
	}
	return true
}

func (gn *GroupNode) IsEqual(n Node) bool {
	if !gn.node.IsEqual(n) {
		return false
	}
	// types match so we have a GroupNode
	fields, err := n.Fields()
	if err != nil {
		return false
	}
	return gn.matchFields(fields)
}

func (gn *GroupNode) Print() string {
	head := gn.name + ":" + gn.typ.String() + ":" + gn.logicalType.String()
	fields := ""
	for _, f := range gn.fields {
		if fields != "" {
			fields += ", "
		}
		fields += f.Print()
	}
	return head + "[" + fields + "]"
}

type ListNode struct {
	GroupNode
}

func NewListNode(name string, repetition parquet.Repetition, element Node) *ListNode {
	// According to https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists:
	// LIST must always annotate a 3-level structure:
	// <list-repetition> group <name> (LIST) {
	//   repeated group list {
	// 	   <element-repetition> <element-type> element;
	//   }
	// }
	//
	// but the arrow-go/parquet library doesn't seem to support this 3-level structure
	//
	// middleNode := NewGroupNode("list", parquet.Repetitions.Repeated, []Node{element}, LogicalTypeNone)
	return &ListNode{
		GroupNode: GroupNode{
			node: node{
				name:        name,
				typ:         NodeTypeNone,
				repetition:  repetition,
				logicalType: LogicalTypeList,
			},
			fields: []Node{element},
		},
	}
}

func (ln *ListNode) Element() Node {
	return ln.fields[0]
}
