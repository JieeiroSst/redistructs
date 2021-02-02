package util

import (
	"fmt"
	"reflect"
	"strings"
)

type Tag struct {
	Name string
	Val  interface{}
	Tags []string
}

func ExtractTags(iface interface{}, name string, tags ...string) (string, []Tag, error) {
	target := IndirectValue(iface)

	if target.Kind() != reflect.Struct {
		return "", nil, fmt.Errorf("cannot extract tags from non-struct type")
	}

	extracted := []Tag{}

	for i := 0; i < target.NumField(); i++ {
		fType := target.Type().Field(i)

		if tagStr, ok := fType.Tag.Lookup(name); ok {
			inputTags := strings.Split(tagStr, ",")
			validTags := StringsIntersects(inputTags, tags)

			if len(validTags) > 0 {
				fVal := IndirectValue(target.Field(i).Interface())

				extracted = append(extracted, Tag{
					Name: fType.Name,
					Val:  fVal.Interface(),
					Tags: validTags,
				})
			}
		}
	}
	return target.Type().Name(), extracted, nil
}

func IndirectValue(iface interface{}) reflect.Value {
	target := reflect.ValueOf(iface)
	for target.Kind() == reflect.Ptr {
		target = reflect.Indirect(target)
	}
	return target
}
