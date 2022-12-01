import codegrade
import yaml
import click


def load_config(config_file):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    
    return config

@click.command()
@click.option('--username', required=True, help='The username to lookup')
def run(username):
    config = load_config("config.yaml")
    basepath = "test"
    with codegrade.login(username=config['username'], password=config['password'], tenant=config['tenant']) as client:
        print(client.user.search(q=username))
    


if __name__=="__main__":
    run()