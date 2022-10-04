package elastic

import (
	"encoding/json"
	"errors"
	"strings"
)

var allowType = []string{"text", "number", "array", "date"}
var allowText = []string{"eq", "neq", "like", "nlike"}
var allowNumber = []string{"eq", "neq", "lt", "lte", "gt", "gte"}
var allowArray = []string{"in", "nin"}
var allowDate = []string{"lt", "lte", "gt", "gte"}
var allowLogicalOperators = []string{"and", "or"}
var allowMustNot = []string{"neq", "nlike", "nin"}

type Condition struct {
	Type                string // text, number, array, date
	ComparisonOperators string // eq, neq, in, nin, like, nlike, lt, lte, gt, gte
	LogicalOperators    string // and, or
	Key                 string
	Value               interface{}
}

type Elastic struct {
	Query  Query       `json:"query"`
	Params []Condition `json:"input"`
}

type Query struct {
	Query Bool `json:"query"`
}

type Bool struct {
	Bool BoolQuery `json:"bool"`
}

type BoolQuery struct {
	Must    []interface{} `json:"must,omitempty"`
	MustNot []interface{} `json:"must_not,omitempty"`
	Should  []interface{} `json:"should,omitempty"`
}

//func main() {
//	conds := []Condition{
//		{
//			Type:                "text",
//			ComparisonOperators: "eq",
//			LogicalOperators:    "and",
//			Key:                 "fullName",
//			Value:               "dvt",
//		},
//		{
//			Type:                "text",
//			ComparisonOperators: "neq",
//			LogicalOperators:    "and",
//			Key:                 "fullName",
//			Value:               "nva",
//		},
//		{
//			Type:                "text",
//			ComparisonOperators: "like",
//			LogicalOperators:    "and",
//			Key:                 "summary",
//			Value:               "already",
//		},
//		{
//			Type:                "text",
//			ComparisonOperators: "nlike",
//			LogicalOperators:    "and",
//			Key:                 "summary",
//			Value:               "already",
//		},
//		{
//			Type:                "text",
//			ComparisonOperators: "nlike",
//			LogicalOperators:    "or",
//			Key:                 "summary",
//			Value:               "already",
//		},
//	}
//
//	q := New(conds)
//	rs, err := q.ParseToQuery()
//	fmt.Println("start==========")
//	fmt.Printf("err: %v", err)
//	fmt.Println("")
//	fmt.Printf("rs: %v", string(rs))
//	fmt.Println("")
//	fmt.Println("end==========")
//}

func New(in []Condition) *Elastic {
	return &Elastic{Params: in}
}

func (e *Elastic) ParseToQuery() (query []byte, err error) {
	in := e.Params
	err = validate(in)
	in = toLower(in)
	if err != nil {
		return
	}

	for i := 0; i < len(in); i++ {
		cond := in[i]
		err = e.parseToDSLQuery(cond)
		if err != nil {
			return
		}
	}

	query, err = json.Marshal(e.Query.Query)
	return
}

func (e *Elastic) parseToDSLQuery(in Condition) (err error) {
	operator := in.ComparisonOperators
	logicalOperators := in.LogicalOperators
	params, err := parseComparisonOperators(in)
	if err != nil {
		return
	}

	if contains[string](allowMustNot, operator) {
		e.Query.Query.Bool.MustNot = append(e.Query.Query.Bool.MustNot, params)
		return
	}

	switch logicalOperators {
	case "and":
		e.Query.Query.Bool.Must = append(e.Query.Query.Bool.Must, params)
		return
	case "or":
		e.Query.Query.Bool.Should = append(e.Query.Query.Bool.Should, params)
		return
	default:
		err = errors.New("unsupported comparison operators")
	}
	return
}

func parseComparisonOperators(in Condition) (rs map[string]interface{}, err error) {
	rs = make(map[string]interface{})
	var operator, key = in.ComparisonOperators, in.Key
	var value = in.Value
	switch operator {
	case "eq", "neq":
		rs["term"] = map[string]interface{}{
			key: value,
		}
		return
	case "in", "nin":
		rs["terms"] = map[string]interface{}{
			key: value,
		}
		return
	case "like", "nlike":
		rs["match"] = map[string]interface{}{
			key: value,
		}
		return
	case "lt", "lte", "gt", "gte":
		rs["range"] = map[string]interface{}{
			key: map[string]interface{}{
				operator: value,
			},
		}
		return
	default:
		err = errors.New("unsupported comparison operators")
	}
	return
}

func validate(in []Condition) (err error) {
	for i := 0; i < len(in); i++ {
		cond := in[i]
		if !contains[string](allowType, cond.Type) {
			err = errors.New("unsupported data type")
			break
		}
		if !contains[string](allowLogicalOperators, cond.LogicalOperators) {
			err = errors.New("unsupported logical operators")
			break
		}

		condComparisonOperators := cond.ComparisonOperators
		switch cond.Type {
		case "text":
			if !contains[string](allowText, condComparisonOperators) {
				err = errors.New("unsupported comparison operators for text")
				break
			}
			break
		case "number":
			if !contains[string](allowNumber, condComparisonOperators) {
				err = errors.New("unsupported comparison operators for number")
				break
			}
			break
		case "array":
			if !contains[string](allowArray, condComparisonOperators) {
				err = errors.New("unsupported comparison operators for array")
				break
			}
			break
		case "date":
			if !contains[string](allowDate, condComparisonOperators) {
				err = errors.New("unsupported comparison operators for date")
				break
			}
			break
		}
	}
	return
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}

func toLower(in []Condition) (rs []Condition) {
	rs = make([]Condition, len(in))
	for i := 0; i < len(in); i++ {
		cond := in[i]
		condType := strings.ToLower(cond.Type)
		condLogicalOperators := strings.ToLower(cond.LogicalOperators)
		condComparisonOperators := strings.ToLower(cond.ComparisonOperators)
		rs[i] = Condition{
			Type:                condType,
			ComparisonOperators: condComparisonOperators,
			LogicalOperators:    condLogicalOperators,
			Key:                 cond.Key,
			Value:               cond.Value,
		}
	}
	return
}
