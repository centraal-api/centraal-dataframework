import json
import great_expectations as gx

class Validator:
    funct: str
    src: str

    def __init__(self, funct: str, src: str):
        self.funct = funct
        self.src = src

    def do_success(self):
        context = gx.get_context()
        validator = context.sources.pandas_default.read_csv(
        "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv")
        validator.expect_column_values_to_not_be_null("pickup_datetime")
        validator.expect_column_values_to_be_between("passenger_count", auto=True)
        validator.save_expectation_suite()
        checkpoint = context.add_or_update_checkpoint(
        name="my_quickstart_checkpoint",
        validator=validator,
        )
        checkpoint_result = checkpoint.run()
        return json.dumps(checkpoint_result.to_json_dict())

    def do_fail(self):
        context = gx.get_context()
        validator = context.sources.pandas_default.read_csv(
        "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv")
        
        validator.expect_column_values_to_not_be_null("congestion_surcharge")
        validator.expect_column_values_to_be_between("passenger_count", auto=True)
        validator.save_expectation_suite()
        checkpoint = context.add_or_update_checkpoint(
        name="my_quickstart_checkpoint",
        validator=validator,
        )
        checkpoint_result = checkpoint.run()
        return json.dumps(checkpoint_result.to_json_dict())

    
    def execute(self):
        do = f"do_{self.funct}"
        ret = f'{{"status":"Tarea no registrada"}}'
        if hasattr(self, do) and callable(func := getattr(self, do)):
            ret = func()
        return ret
