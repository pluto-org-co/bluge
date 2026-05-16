package documents

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_TextFieldsFromDefinitions(t *testing.T) {
	assertions := assert.New(t)

	defs := []*FieldDefinition{
		NewTextFieldDefinition("name", "John"),
		NewInt64FieldDefinition("age", 60),
		NewTimeFieldDefinition("contract-expiration", new(time.Now().Add(1000*24*time.Hour))),
		NewTextFieldDefinition("job", "Software Engineer"),
		NewKeywordFieldDefinition("school", "UNAL"),
	}

	var expectedSize int
	for _, def := range defs {
		expectedSize += len(def.Name) + len(def.Value)
	}

	info, fields := FieldsFromDefinitions(defs...)
	if !assertions.Equal(len(info.Buffer), expectedSize, "expecting buffer of exact size") {
		return
	}

	fmt.Println(string(info.Buffer))

	for index, field := range fields {
		if !assertions.Equal(defs[index].Name, field.name, "field name doesn't match") {
			return
		}
	}
}
