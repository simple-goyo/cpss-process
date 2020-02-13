# -*-coding:utf8-*-


def get_id_str(apps):
    if isinstance(apps, dict):
        apps["_id"] = str(apps["_id"])
        # if apps["_id"]:
        #     apps["_id"] = str(apps["_id"])
        # if apps["app_class_id"]:
        #     apps["app_class_id"] = str(apps["app_class_id"])
    else:
        for app in apps:
            app["_id"] = str(app["_id"])
            # if app["_id"]:
            #     app["_id"] = str(app["_id"])
            # if app["app_class_id"]:
            #     app["app_class_id"] = str(app["app_class_id"])
    return apps
