package structtag

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

func TestExtractValues(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		encodedArgs    []byte
		uniqueKeys     []string
		expectedValues []string
	}{
		{
			name:           "SimpleStringFields",
			encodedArgs:    []byte(`{"name":"alice","email":"alice@example.com","age":30}`),
			uniqueKeys:     []string{"name", "email"},
			expectedValues: []string{`"alice"`, `"alice@example.com"`},
		},
		{
			name:           "NumericField",
			encodedArgs:    []byte(`{"name":"bob","count":42}`),
			uniqueKeys:     []string{"count"},
			expectedValues: []string{"42"},
		},
		{
			name:           "BooleanField",
			encodedArgs:    []byte(`{"active":true,"disabled":false}`),
			uniqueKeys:     []string{"active", "disabled"},
			expectedValues: []string{"true", "false"},
		},
		{
			name:           "NestedField",
			encodedArgs:    []byte(`{"user":{"name":"charlie","address":{"city":"NYC"}}}`),
			uniqueKeys:     []string{"user.name", "user.address.city"},
			expectedValues: []string{`"charlie"`, `"NYC"`},
		},
		{
			name:           "MissingField",
			encodedArgs:    []byte(`{"name":"dave"}`),
			uniqueKeys:     []string{"name", "missing"},
			expectedValues: []string{`"dave"`, "undefined"},
		},
		{
			name:           "NullValue",
			encodedArgs:    []byte(`{"name":null}`),
			uniqueKeys:     []string{"name"},
			expectedValues: []string{"null"},
		},
		{
			name:           "ObjectValue",
			encodedArgs:    []byte(`{"user":{"id":1,"name":"eve"}}`),
			uniqueKeys:     []string{"user"},
			expectedValues: []string{`{"id":1,"name":"eve"}`},
		},
		{
			name:           "ArrayValue",
			encodedArgs:    []byte(`{"tags":["a","b","c"]}`),
			uniqueKeys:     []string{"tags"},
			expectedValues: []string{`["a","b","c"]`},
		},
		{
			name:           "EmptyKeys",
			encodedArgs:    []byte(`{"name":"frank"}`),
			uniqueKeys:     []string{},
			expectedValues: []string{},
		},
		{
			name:           "AllMissing",
			encodedArgs:    []byte(`{}`),
			uniqueKeys:     []string{"a", "b"},
			expectedValues: []string{"undefined", "undefined"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actualValues := ExtractValues(tt.encodedArgs, tt.uniqueKeys)
			require.Equal(t, tt.expectedValues, actualValues)
		})
	}
}

func TestSortedFieldsWithTag(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		argsFunc       func() rivertype.JobArgs
		tagValue       string
		expectedFields []string
	}{
		{
			name: "SimpleUniqueFields",
			argsFunc: func() rivertype.JobArgs {
				type SimpleArgs struct {
					testutil.JobArgsReflectKind[SimpleArgs]

					Name  string `json:"name"  river:"unique"`
					Email string `json:"email" river:"unique"`
					Age   int    `json:"age"`
				}

				return SimpleArgs{}
			},
			tagValue:       "unique",
			expectedFields: []string{"email", "name"},
		},
		{
			name: "NestedStructWithUniqueFields",
			argsFunc: func() rivertype.JobArgs {
				type Inner struct {
					FieldA string `json:"field_a" river:"unique"`
					FieldB int    `json:"field_b"`
				}

				type NestedOuter struct {
					testutil.JobArgsReflectKind[NestedOuter]

					InnerField Inner  `json:"inner_field"`
					FieldC     string `json:"field_c"     river:"unique"`
					FieldD     int    `json:"field_d"     river:"unique,otheroption"`
				}

				return NestedOuter{}
			},
			tagValue:       "unique",
			expectedFields: []string{"field_c", "field_d", "inner_field.field_a"},
		},
		{
			name: "NoUniqueFields",
			argsFunc: func() rivertype.JobArgs {
				type NoUniqueArgs struct {
					testutil.JobArgsReflectKind[NoUniqueArgs]

					Name string `json:"name"`
					Age  int    `json:"age"`
				}

				return NoUniqueArgs{}
			},
			tagValue:       "unique",
			expectedFields: nil,
		},
		{
			name: "NoJSONTagUsesFieldName",
			argsFunc: func() rivertype.JobArgs {
				type NoJSONTagArgs struct {
					testutil.JobArgsReflectKind[NoJSONTagArgs]

					Name string `river:"unique"`
				}

				return NoJSONTagArgs{}
			},
			tagValue:       "unique",
			expectedFields: []string{"Name"},
		},
		{
			name: "DifferentTagValue",
			argsFunc: func() rivertype.JobArgs {
				type DifferentTagArgs struct {
					testutil.JobArgsReflectKind[DifferentTagArgs]

					Field1 string `json:"field1" river:"special"`
					Field2 string `json:"field2" river:"unique"`
				}

				return DifferentTagArgs{}
			},
			tagValue:       "special",
			expectedFields: []string{"field1"},
		},
		{
			name: "AnonymousEmbeddedStruct",
			argsFunc: func() rivertype.JobArgs {
				type EmbeddedBase struct {
					BaseField string `json:"base_field" river:"unique"`
				}

				type WithEmbeddedArgs struct {
					testutil.JobArgsReflectKind[WithEmbeddedArgs]
					EmbeddedBase

					OwnField string `json:"own_field" river:"unique"`
				}

				return WithEmbeddedArgs{}
			},
			tagValue:       "unique",
			expectedFields: []string{"base_field", "own_field"},
		},
		{
			name: "DeeplyNestedStruct",
			argsFunc: func() rivertype.JobArgs {
				type DeepNested struct {
					Value string `json:"value" river:"unique"`
				}

				type MiddleLevel struct {
					Deep DeepNested `json:"deep"`
				}

				type DeepNestedArgs struct {
					testutil.JobArgsReflectKind[DeepNestedArgs]

					Middle MiddleLevel `json:"middle"`
				}

				return DeepNestedArgs{}
			},
			tagValue:       "unique",
			expectedFields: []string{"middle.deep.value"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actualFields, err := SortedFieldsWithTag(tt.argsFunc(), tt.tagValue)
			require.NoError(t, err)
			require.Equal(t, tt.expectedFields, actualFields)
		})
	}
}
