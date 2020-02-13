import requests
from bson.json_util import loads, dumps
import json

# 全部
resource_id_list_all = [
    {
        "uid": "736824e6-6765-4c19-8c8b-86cece767a5a",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "coffee-maker-fudan"
    },
    {
        "uid": "39b96cc8-5990-4dca-9499-49cffbccf5b8",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "coffee-maker-nju",
    },
    {
        "uid": "cb20571a-17cd-4534-9f5e-0dd98a8e1d48",
        "namespace": "fusion-app-resources",
        "kind": "Service",
        "phase": "NotReady",
        "name": "com-gotokeep-keep-service",
    },
    {
        "uid": "faa3f1ac-95dd-4cb2-81a2-4fb7e1ad16fb",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "kettle-mi",
    },
    {
        "uid": "b32f954e-7ede-4c65-9d36-99e0a2f25869",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "purifier-mi",
    },
    {
        "uid": "9650d166-c299-4465-8f93-bca5aa55df3b",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "smartbox-haier",
    },
    {
        "uid": "bed4c157-79fb-4016-b50c-69f18122b619",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "speaker-mi",
    },
    {
        "uid": "0b1b9982-5638-4053-80bc-4057a052efba",
        "namespace": "fusion-app-resources",
        "kind": "Human",
        "name": "user-001",
    },
    {
        "uid": "317ce939-3a0a-4d65-85dd-9a488365d4ed",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "weight-lexin",
    },
    {
        "uid": "489a0655-0dc5-4cc7-acf8-feccde447b86",
        "namespace": "fusion-app-resources",
        "kind": "Service",
        "name": "starbuck-service",
    },
    {
        "uid": "1ff79dbc-2562-45d9-bd07-2ce8d7b34685",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "tv",
    },
    {
        "uid": "2f5483a4-a3c2-44dc-8259-abffa7c478a0",
        "namespace": "fusion-app-resources",
        "kind": "Human",
        "name": "human13860560456",
    }
]

# 语音喝咖啡
resource_id_list_get_coffee_voice = [
    {
        "uid": "736824e6-6765-4c19-8c8b-86cece767a5a",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "coffee-maker-fudan"
    },
    {
        "uid": "bed4c157-79fb-4016-b50c-69f18122b619",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "speaker-mi",
    },
    {
        "uid": "2f5483a4-a3c2-44dc-8259-abffa7c478a0",
        "namespace": "fusion-app-resources",
        "kind": "Human",
        "name": "human13860560456",
    }
]

# 人送咖啡
resource_id_list_get_coffee_people = [
    {
        "uid": "39b96cc8-5990-4dca-9499-49cffbccf5b8",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "coffee-maker-nju",
    },
    {
        "uid": "bed4c157-79fb-4016-b50c-69f18122b619",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "speaker-mi",
    },
    {
        "uid": "2f5483a4-a3c2-44dc-8259-abffa7c478a0",
        "namespace": "fusion-app-resources",
        "kind": "Human",
        "name": "human13860560456",
    }
]

# 锻炼屋
resource_id_list_exercise_room = [

    {
        "uid": "cb20571a-17cd-4534-9f5e-0dd98a8e1d48",
        "namespace": "fusion-app-resources",
        "kind": "Service",
        "phase": "NotReady",
        "name": "com-gotokeep-keep-service",
    },
    {
        "uid": "faa3f1ac-95dd-4cb2-81a2-4fb7e1ad16fb",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "kettle-mi",
    },
    {
        "uid": "b32f954e-7ede-4c65-9d36-99e0a2f25869",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "purifier-mi",
    },
    {
        "uid": "9650d166-c299-4465-8f93-bca5aa55df3b",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "smartbox-haier",
    },
    {
        "uid": "317ce939-3a0a-4d65-85dd-9a488365d4ed",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "weight-lexin",
    },
    {
        "uid": "1ff79dbc-2562-45d9-bd07-2ce8d7b34685",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "tv",
    }
]

# 星巴克咖啡
resource_id_list_starkbuck = [
    {
        "uid": "bed4c157-79fb-4016-b50c-69f18122b619",
        "namespace": "fusion-app-resources",
        "kind": "Edge",
        "name": "speaker-mi",
    },
    {
        "uid": "489a0655-0dc5-4cc7-acf8-feccde447b86",
        "namespace": "fusion-app-resources",
        "kind": "Service",
        "name": "starbuck-service",
    }
]


def unbind(resource_id):
    data = {
        "refResource": resource_id
    }
    reponse = requests.delete("http://221.228.66.83:32022/apis/resource/bind", data=dumps(data))
    # output = loads(reponse.content)
    return reponse


app_unbind_list = {
    "0": resource_id_list_all,
    "1": resource_id_list_get_coffee_voice,
    "2": resource_id_list_get_coffee_people,
    "3": resource_id_list_exercise_room,
    "4": resource_id_list_starkbuck
}

if __name__ == '__main__':
    resource_id_list = app_unbind_list["0"]
    for resource_id in resource_id_list:
        unbind(resource_id)
