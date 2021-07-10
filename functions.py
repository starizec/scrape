from variables import *

def printLog(log_str, env = "all"):
    if env == environment or env == "all":
        print(log_str)