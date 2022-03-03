import os

for topic in os.getenv("TOPIC_LIST").split(" "):
    print(f"Creating topic {topic}")
    os.system("/usr/src/app/")