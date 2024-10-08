package schema

import (
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

type NodeType int

const (
	NodeTypeBoolean NodeType = 0
	NodeTypeInt64   NodeType = 1
	NodeTypeFloat64 NodeType = 2
)

type BaseNode struct {
	Name       string             `json:"name"`
	Type       NodeType           `json:"type"`
	Repetition parquet.Repetition `json:"repetition"`
}

func (bn *BaseNode) GetName() string {
	return bn.Name
}

func (bn *BaseNode) GetType() NodeType {
	return bn.Type
}

func (bn *BaseNode) GetRepetition() parquet.Repetition {
	return bn.Repetition
}

func (bn *BaseNode) SetRepetition(repetition parquet.Repetition) {
	bn.Repetition = repetition
}

type BooleanNode struct {
	BaseNode
}

func NewBooleanNode(name string, repetition parquet.Repetition) *BooleanNode {
	return &BooleanNode{
		BaseNode{
			Name:       name,
			Type:       NodeTypeBoolean,
			Repetition: repetition,
		},
	}
}

func (bn *BooleanNode) Node() schema.Node {
	return schema.NewBooleanNode(bn.Name, bn.GetRepetition(), -1)
}

type Int64Node struct {
	BaseNode
}

func NewInt64Node(name string, repetition parquet.Repetition) *Int64Node {
	return &Int64Node{
		BaseNode{
			Name:       name,
			Type:       NodeTypeInt64,
			Repetition: repetition,
		},
	}
}

func (bn *Int64Node) Node() schema.Node {
	return schema.NewInt64Node(bn.Name, bn.GetRepetition(), -1)
}

type Float64Node struct {
	BaseNode
}

func NewFloat64Node(name string, repetition parquet.Repetition) *Float64Node {
	return &Float64Node{
		BaseNode{
			Name:       name,
			Type:       NodeTypeFloat64,
			Repetition: repetition,
		},
	}
}

func (bn *Float64Node) Node() schema.Node {
	return schema.NewFloat64Node(bn.Name, bn.GetRepetition(), -1)
}
