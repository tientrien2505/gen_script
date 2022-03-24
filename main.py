import json

TAB_SPACE = '    '


class FunctionType:
    NORMAL = 'normal'
    TRANSFORMATION = 'transformation'


def gen_test():
    with open('config.json', 'r') as f:
        str_config = f.read()
    config = json.loads(str_config)
    filename = config['filename']
    class_name = config['class']

    content = f"""import pytest
from pyspark.sql import SparkSession
from share.spark.testing.utils import assert_dataframe_equals
from .{filename} import {class_name}


"""
    tested_functions = config['functions']
    function_block = ''
    for index, func in enumerate(tested_functions):
        function_block += f'@pytest.mark.parametrize("test_case", [\n' \
                         f'{TAB_SPACE}dict(\n' \
                         f'{TAB_SPACE*2}name="test schema",\n'
        for ip in func['input']:
            function_block += f'{TAB_SPACE*2}{ip["name"]}=[],\n'
        function_block += f'{TAB_SPACE*2}expected=[],\n'
        function_block += f'{TAB_SPACE})\n'
        function_block += f'])\n'
        function_block += f'def test_{func["name"]}(spark: SparkSession, test_case):\n'
        for ip in func['input']:
            if ip['type'] == 'df':
                function_block += f'{TAB_SPACE}{ip["name"]} = spark.createDataFrame(\n' \
                                  f'{TAB_SPACE*2}test_case["{ip["name"]}"],\n' \
                                  f'{TAB_SPACE*2}schema={class_name}.INPUT_SCHEMA["{ip["name"]}"]\n' \
                                  f'{TAB_SPACE})\n'
        # expected df
        function_block += f'{TAB_SPACE}expected_df = spark.createDataFrame(test_case["expected"], schema={class_name}.OUTPUT_SCHEMA)\n'
        # actual df
        function_type = func['type']
        if function_type == FunctionType.TRANSFORMATION:
            first_df = func['input'][0]
            function_block += f'{TAB_SPACE}actual_df = (\n' \
                              f'{TAB_SPACE*2}{first_df["name"]}\n' \
                              f'{TAB_SPACE*2}.transform(\n' \
                              f'{TAB_SPACE*3}{class_name}.{func["name"]}(\n'
            for i, ip in enumerate(func['input']):
                if i == 0:
                    continue
                function_block += f'{TAB_SPACE*4}{ip["name"]}={ip["name"]},\n'
            function_block += f'{TAB_SPACE*3})\n'
            function_block += f'{TAB_SPACE*2})\n'
            function_block += f'{TAB_SPACE*1})\n'
        else:
            function_block += f'{TAB_SPACE}actual_df = {class_name}.{func["name"]}(\n'

            for ip in func['input']:
                function_block += f'{TAB_SPACE*2}{ip["name"]}={ip["name"]},\n'
            function_block += f'{TAB_SPACE})\n'
        function_block += f'{TAB_SPACE}assert_dataframe_equals(actual_df, expected_df, test_case["name"])\n'
        if index < len(tested_functions) - 1:
            function_block += '\n\n'

    content += function_block
    with open(f'{filename}_test.py', 'w') as f:
        f.write(content)


if __name__ == '__main__':
    gen_test()
