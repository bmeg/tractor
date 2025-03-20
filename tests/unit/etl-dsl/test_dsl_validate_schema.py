import jsonschema


# Define a function to validate the schema
def test_validate_schema(schema):
    jsonschema.Draft7Validator.check_schema(schema)

