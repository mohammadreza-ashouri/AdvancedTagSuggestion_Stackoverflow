from app import mongodb
from bson.objectid import ObjectId


def load_module_list():
    modules = mongodb.db.modules.find({})
    return modules


def insert_module(module_name):
    new_module_obj = {
        "name": module_name
    }
    module_obj = mongodb.db.modules.find_one(new_module_obj)
    if module_obj is None:
        object_id = mongodb.db.modules.insert_one(new_module_obj).inserted_id
        return object_id, "new"
    else:
        return str(module_obj["_id"]), "update"


def insert_module_detail(module_name, module_detail):
    new_module_obj = {
        "name": module_name,
    }
    module_obj = mongodb.db.modules.find_one(new_module_obj)
    new_module_obj["detail"] = module_detail
    if module_obj is None:

        object_id = mongodb.db.modules.insert_one(new_module_obj).inserted_id
        return object_id, "new"
    else:
        mongodb.db.modules.update_one(
            filter={"_id": ObjectId(module_obj["_id"])},
            update={
                "$set": {
                    "detail": module_detail
                }
            },
            upsert=True
        )
        return ObjectId(module_obj["_id"]), "update"


def remove_module(module_id):

    module_obj = mongodb.db.modules.find_one({"_id": ObjectId(module_id)})
    if module_obj is not None:
        mongodb.db.modules.remove({"_id": ObjectId(module_id)})
        return True
    return False


def get_module_detail(module_name):
    module_dir = dir(module_name)

    func_dict = {}
    print(module_dir)

    for module_func in module_dir:

        attr = getattr(module_name, module_func, None)
        help_txt = str(attr.__doc__)
        lines = help_txt.split('\n\n')

        doc = lines[0]
        if len(lines) > 1:
            doc = lines[len(lines)-1]

        func_dict[module_func] = doc

    mid, status = insert_module_detail(module_name, func_dict)
    return mid, status
