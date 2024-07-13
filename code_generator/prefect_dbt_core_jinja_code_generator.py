from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from prefect_dbt.cli import DbtCoreOperation
import subprocess
from prefect.filesystems import GitHub
from dotenv import load_dotenv
import re

# for local testing
load_dotenv()


class GeneratePrefectDbtCoreJinjaTemplate():
    # dont edit beflow var, they are immutable
    _template_name: str= 'prefect_dbt_core_jinja_template.txt'
    _prefect_agent_dbt_github_name = "github-prefect-dbt-core-orchestration"
    _prefect_agent_dbt_github = GitHub.load(_prefect_agent_dbt_github_name)
    github_repo_name =  re.search(r'[a-zA-z0-9]+',re.search(r'\/[a-zA-z0-9]+.git',_prefect_agent_dbt_github.repository).group()).group()
    github_branch = _prefect_agent_dbt_github.reference
    

    def __init__(self, dbt_core_object_name) -> None:
        self.dbt_core_object_name = dbt_core_object_name
        self.dbt_core_object: object = DbtCoreOperation.load(self.dbt_core_object_name)
        self.file_name = self._create_file_name()
        self._write_file_location = self._set_write_file_location()
        self.file_location = Path.joinpath(self._write_file_location, self.file_name)

    def _set_write_file_location(self):
        if self.github_repo_name not in str(Path.cwd().parent):
            _write_file_location = Path.cwd() / self.github_repo_name
        else:
            _write_file_location = Path.cwd().parent
        return _write_file_location
    def _create_file_name(self):
        return 'dbt_repo_' + self.dbt_core_object_name.replace('-', '_') +'.py'

    def generate_prefect_dbt_core_jinja_template(self):
        commands_of_dbt_core_object_as_dict: dict = {k: v for k, v in enumerate(self.dbt_core_object.commands, start=1)}
        template_location = ['jinja_template', Path.joinpath(self._write_file_location,'code_generator/jinja_template')]
        r: object = Environment(
            loader=FileSystemLoader(template_location), 
            extensions=['jinja2.ext.do']
        )
        load_template = r.get_template(self._template_name)

        with open(Path.joinpath(self._write_file_location, self.file_name), 'w') as f:
            f.write(load_template.render(
                dbt_core_object_name_in_prefect = self.dbt_core_object_name,
                commands_of_dbt_core_object_in_prefect_as_dict = commands_of_dbt_core_object_as_dict,
                prefect_deployement_name = self.file_name.replace('.py', ''),
                prefect_agent_dbt_github_name=self._prefect_agent_dbt_github_name
            ))

    def push_generated_template_to_prefect_agent_dbt_github(self):
        command = f'git add --all && git commit -m "update {self.file_name}" && git push origin {self.github_branch}'
        run = subprocess.run(command, shell=True, cwd=self._write_file_location, capture_output=True)
        return run

    def create_prefect_deployment(self,dbt_command):
        command = f'python {self.file_name} --deploy "true" --command "{dbt_command}"'
        run = subprocess.run(command, shell=True, cwd=self._write_file_location, capture_output=True, check=True)
        return run


if __name__ == "__main__":
    pass


