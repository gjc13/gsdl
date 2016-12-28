package frontend

import (
	"fmt"
	"math"

	core "github.com/gjc13/gsdl/core"
	view "github.com/gjc13/gsdl/view"
)

func minReduceView(colName string, v view.Viewer) int {
	colId := view.ColumnName2Id(colName, v.ColumnNames())
	c := make(chan []interface{})
	go v.Iter(c)
	minVal := math.MaxInt64
	for row := range c {
		if int(core.ToInt64(row[colId])) < minVal {
			minVal = int(core.ToInt64(row[colId]))
		}
	}
	return int(minVal)
}

func maxReduceView(colName string, v view.Viewer) int {
	colId := view.ColumnName2Id(colName, v.ColumnNames())
	c := make(chan []interface{})
	go v.Iter(c)
	maxVal := math.MinInt64
	for row := range c {
		if int(core.ToInt64(row[colId])) > maxVal {
			maxVal = int(core.ToInt64(row[colId]))
		}
	}
	return int(maxVal)
}

func sumReduceView(colName string, v view.Viewer) (int, int) {
	colId := view.ColumnName2Id(colName, v.ColumnNames())
	c := make(chan []interface{})
	go v.Iter(c)
	sum := 0
	cnt := 0
	for row := range c {
		cnt += 1
		sum += int(core.ToInt64(row[colId]))
	}
	return sum, cnt
}

func toGroups(reduceColName string, groupColName string, v view.Viewer) map[string][]int {
	reduceColId := view.ColumnName2Id(reduceColName, v.ColumnNames())
	groupColId := view.ColumnName2Id(groupColName, v.ColumnNames())
	groupMap := make(map[string][]int)
	c := make(chan []interface{})
	go v.Iter(c)
	for row := range c {
		key := fmt.Sprintf("%v", row[groupColId])
		groupMap[key] = append(groupMap[key], int(core.ToInt64(row[reduceColId])))
	}
	return groupMap
}

func minGroupView(reduceColName string, colName string, v view.Viewer) (groupKeys []string, groupMins []int) {
	groupMap := toGroups(reduceColName, colName, v)
	for k, vals := range groupMap {
		groupKeys = append(groupKeys, k)
		minVal := int(math.MaxInt64)
		for _, v := range vals {
			if v < minVal {
				minVal = v
			}
		}
		groupMins = append(groupMins, minVal)
	}
	return
}

func maxGroupView(reduceColName string, colName string, v view.Viewer) (groupKeys []string, groupMaxs []int) {
	groupMap := toGroups(reduceColName, colName, v)
	for k, vals := range groupMap {
		groupKeys = append(groupKeys, k)
		maxVal := int(math.MinInt64)
		for _, v := range vals {
			if v > maxVal {
				maxVal = v
			}
		}
		groupMaxs = append(groupMaxs, maxVal)
	}
	return
}

func sumGroupView(reduceColName string, colName string, v view.Viewer) (groupKeys []string, groupSums []int, groupCnts []int) {
	groupMap := toGroups(reduceColName, colName, v)
	for k, vals := range groupMap {
		groupKeys = append(groupKeys, k)
		groupCnts = append(groupCnts, len(vals))
		sum := 0
		for _, v := range vals {
			sum += v
		}
		groupSums = append(groupSums, sum)
	}
	return
}
