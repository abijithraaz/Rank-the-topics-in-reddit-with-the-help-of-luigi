import configparser

config_path = "configuration/reddit_config.ini"

def reddit_config_load(config_name):
    """
    Loads configuration from the config file
    """
    reddit_config = configparser.ConfigParser()
    reddit_config.read(config_path)
    return reddit_config[config_name]
