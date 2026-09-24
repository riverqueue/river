package harness_test

import (
	"encoding/json"
	"fmt"
	"os"
)

// adapterContract is conformance/adapter/contract.json with lookups for
// validating requests and responses.
type adapterContract struct {
	errorNames map[int]string
	errorCodes map[string]int
	methods    map[string]int
	path       string
	validator  *schemaValidator
}

func parseAdapterContract(path string) (*adapterContract, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var decoded struct {
		Errors []struct {
			Code int    `json:"code"`
			Name string `json:"name"`
		} `json:"errors"`
		Methods []struct {
			Name string `json:"name"`
		} `json:"methods"`
	}
	if err := json.Unmarshal(contents, &decoded); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	contract := &adapterContract{
		errorCodes: make(map[string]int, len(decoded.Errors)),
		errorNames: make(map[int]string, len(decoded.Errors)),
		methods:    make(map[string]int, len(decoded.Methods)),
		path:       path,
		validator:  newSchemaValidator(),
	}
	for _, contractError := range decoded.Errors {
		contract.errorCodes[contractError.Name] = contractError.Code
		contract.errorNames[contractError.Code] = contractError.Name
	}
	for index, method := range decoded.Methods {
		contract.methods[method.Name] = index
	}
	return contract, nil
}

// validate checks a method's params or result against its contract schema.
// Methods outside the contract are not validated.
func (contract *adapterContract) validate(method, part string, value any) error {
	index, ok := contract.methods[method]
	if !ok {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	decoded, err := decodeJSONWithNumbers(encoded)
	if err != nil {
		return err
	}
	if err := contract.validator.validateFile(decoded, contract.path, fmt.Sprintf("#/methods/%d/%s", index, part)); err != nil {
		return fmt.Errorf("%s %s do not match the adapter contract: %w", method, part, err)
	}
	return nil
}
