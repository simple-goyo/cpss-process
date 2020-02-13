# -*-coding:utf8-*-
from bson import ObjectId

"""t_app_class
"""


def insert_app_class(t_app_class, app_class):
    resource_id = app_class.get("resourceId")
    # 删除
    myquery = {"resourceId": resource_id}
    t_app_class.delete_one(myquery)
    # 加入
    insert_one_result = t_app_class.insert_one(app_class)
    return insert_one_result.inserted_id


def find_all_app_class_introduction(t_app_class):
    app_classes_introduction = []
    for app_class in t_app_class.find({}, {"properties": 1}):
        app_classes_introduction.append(app_class)
    return app_classes_introduction


def find_all_app_class(t_app_class):
    app_classes = []
    for app_class in t_app_class.find():
        app_classes.append(app_class)
    return app_classes


def find_app_class_by_id(t_app_class, app_class_id):
    app_class = t_app_class.find_one({'_id': ObjectId(app_class_id)})
    return app_class


def get_resource_ids_by_app_class_id(t_app_class, app_class_id, type):
    app_class = t_app_class.find_one({'_id': ObjectId(app_class_id)})
    resource_ids = set()
    for childShape in app_class['childShapes']:
        if type in childShape['properties']:
            # todo 会有多个资源,要分开获取
            result = childShape['properties'][type]
            if type == "activityelement":
                resource_ids.add((result['name'], result['id']))
            elif type == "input":
                if result == "":
                    continue
                for input_param in result:
                    resource_ids.add((input_param['name'], input_param['id']))
            elif type == "output":
                if result == "":
                    continue
                for output_param in result:
                    resource_ids.add((output_param['name'], output_param['id']))
    return resource_ids


def gete_input_resource_ids_by_app_class_id(t_app_class, app_class_id):
    app_class = t_app_class.find_one({'_id': ObjectId(app_class_id)})
    resource_ids = set()
    for childShape in app_class['childShapes']:
        if 'servicetaskclass' in childShape['properties']:
            resource_ids.add(childShape['properties']['name'])
    return resource_ids


def get_output_resource_ids_by_app_class_id(t_app_class, app_class_id):
    app_class = t_app_class.find_one({'_id': ObjectId(app_class_id)})
    resource_ids = set()
    for childShape in app_class['childShapes']:
        if 'servicetaskclass' in childShape['properties']:
            resource_ids.add(childShape['properties']['name'])
    return resource_ids


"""t_app_instance
"""


def insert_app_instance(t_app_instance, app_instance):
    insert_one_result = t_app_instance.insert_one(app_instance)
    return insert_one_result.inserted_id


def insert_app_instance_resource(t_app_instance, app_instance_id, resource_id, resource_instance_id):
    myquery = {"_id": app_instance_id}
    newvalues = {"$set": {"resource." + resource_id: resource_instance_id}}
    t_app_instance.update_one(myquery, newvalues)


def update_app_instance_action_state(t_app_instance, app_instance_id, action_id, action_state):
    myquery = {"_id": ObjectId(app_instance_id)}
    newvalues = {"$set": {"action_state." + action_id: action_state}}
    t_app_instance.update_one(myquery, newvalues)


def update_app_instance_action_time(t_app_instance, app_instance_id, action_id, action_state, action_time):
    myquery = {"_id": ObjectId(app_instance_id)}
    newvalues = {"$set": {"action_state_time." + action_id + "." + action_state: action_time}}
    t_app_instance.update_one(myquery, newvalues)


def update_app_instance_state(t_app_instance, app_instance_id, app_instance_state):
    myquery = {"_id": ObjectId(app_instance_id)}
    newvalues = {"$set": {"app_instance_state": app_instance_state}}
    t_app_instance.update_one(myquery, newvalues)


def update_app_instance_uid(t_app_instance, app_instance_id, uid):
    myquery = {"_id": ObjectId(app_instance_id)}
    newvalues = {"$set": {"uid": uid}}
    t_app_instance.update_one(myquery, newvalues)


def find_app_instance_by_id(t_app_instance, id):
    app_instance = t_app_instance.find_one({'_id': ObjectId(id)})
    return app_instance


def find_app_instance_by_uid(t_app_instance, uid):
    app_instance = t_app_instance.find_one({'uid': uid})
    return app_instance


def find_all_app_instance_introduction(t_app_instance):
    app_instance_introduction = []
    result = t_app_instance.aggregate([
        {
            "$lookup": {
                "from": "t_app_class",
                "localField": "app_class_id",
                "foreignField": "_id",
                "as": "app_class"
            }
        },
        {
            "$unwind": "$app_class"
        },
        {
            "$project": {
                "_id": 1,
                "user_id": 1,
                "create_time": 1,
                "app_instance_state": 1,
                "app_class.properties.name": 1
            }
        }
    ])
    for app_instance in result:
        app_instance_state = app_instance.get("app_instance_state")
        if app_instance_state is not None:
            if app_instance_state is not "0":
                app_instance_introduction.append(app_instance)
    return app_instance_introduction


def find_app_class_by_instance_id(t_app_instance, app_instance_id):
    app_class = []
    result = t_app_instance.aggregate([
        {
            "$lookup": {
                "from": "t_app_class",
                "localField": "app_class_id",
                "foreignField": "_id",
                "as": "app_class"
            }
        },
        {
            "$unwind": "$app_class"
        },
        {
            "$project": {
                "_id": 1,
                "app_class.childShapes": 1,
                "app_class.properties": 1
            }
        },
        {
            "$match": {
                "_id": ObjectId(app_instance_id)
            }
        }
    ])
    for app_instance in result:
        app_class.append(app_instance)
    if len(app_class):
        return app_class[0]
    else:
        return None


def find_app_instance_action_state_by_instance_id(t_app_instance, app_instance_id):
    app_instance_action_state = t_app_instance.find_one({'_id': ObjectId(app_instance_id)},
                                                        {"action_state": 1, "action_state_time": 1})
    return app_instance_action_state


def find_app_instance_action_state_by_uid(t_app_instance, uid):
    app_instance_action_state = t_app_instance.find_one({'_id': ObjectId(uid)}, {"action_state": 1})
    return app_instance_action_state


def find_app_instance_resource_by_instance_id(t_app_instance, app_instance_id):
    app_instance_resource = t_app_instance.find_one({'_id': ObjectId(app_instance_id)}, {"resource": 1})
    return app_instance_resource


def delete_app_instance_by_id(t_app_instance, app_instance_id):
    # 删除
    myquery = {"_id": ObjectId(app_instance_id)}
    t_app_instance.delete_one(myquery)
    return


# app_show
def find_app_show(t_app_show, user_id):
    app_show_instance_id = t_app_show.find_one({'user_id': user_id})
    return app_show_instance_id


def update_t_app_show_instance_id(t_app_show, user_id, instance_id):
    myquery = {"user_id": user_id}
    newvalues = {"$set": {"instance_id": instance_id}}
    t_app_show.update_one(myquery, newvalues)
    return
