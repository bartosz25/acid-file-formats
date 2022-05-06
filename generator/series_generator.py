import os
import sys

from jinja2 import Environment, select_autoescape, FileSystemLoader

output_dir = sys.argv[1]

series_names = [
    '000_api',
    '001_storage_layout',
    '002_schema_evolution',
    '003_compaction'
]
env = Environment(
    loader=FileSystemLoader(searchpath='templates'),
    autoescape=select_autoescape()
)
dirs_to_create = [
    '/src/main/scala/com/waitingforcode',
    '/src/main/resources'
]
acid_to_create = ['apache_hudi', 'apache_iceberg', 'delta_lake']
for series in series_names:
    series_dir = f'{output_dir}/{series}'
    should_override = 'y'
    if os.path.isdir(series_dir):
        should_override = input(
            f'The project for {series} already exists. Do you want to override the pom.xml? [y/n]'
        ).lower()
    for acid_format in acid_to_create:
        pom_template = env.get_template(f'{acid_format}/pom.xml')
        pom_rendered = pom_template.render(series_name=series)

        acid_format_dir = f'{series_dir}/{acid_format}'
        for dir_with_path in dirs_to_create:
            os.makedirs(f'{acid_format_dir}/{dir_with_path}', exist_ok=True)
        if should_override == 'y':
            with open(f'{acid_format_dir}/pom.xml', "w") as fh:
                fh.write(pom_rendered)

