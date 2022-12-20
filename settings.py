##################################################
#       asimov_poc   Settings File
##################################################

## Set the global logging level to debug
#LOGGING_DEBUG = True

## Consumer configuration
### Each consumer has different parameters and can be found in the documentation
CONSUMER_CONFIG = {
    'CLASS': 'apf.consumers.KafkaConsumer',
    'PARAMS': {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'python_example_group_1'
    },
    'TOPICS': ["commands"],
    'NUM_MESSAGES': 5
}

## Step Configuration
STEP_CONFIG = {
    # "N_PROCESS": 4,            # Number of process for multiprocess script
    # "COMMIT": False,           #Disables commit, useful to debug a KafkaConsumer
}