from database import Database
from Permission import Permission


db = Database('test', 'dbo', '1234', True)

'''
if db.Sec_IsOwner():
    print("user is owner")
else:
    print("user is not the owner")
'''

'''
db.Sec_UserAdd('userTest1', 'testPW')
db.Sec_UserAdd('userTest2', 'testPW')
db.Sec_UserAdd('userTest3', 'testPW')
'''

#db.create_table('test1', ['id', 'description'], [int, str])

'''
result = db.Sec_UserAdd('userTest5', 'testPW')
if result==1:
    print("user added successfully")
elif result == 0:
    print("Can NOT add user: current user is not the owner")
elif result == -1:
    print("Can NOT add user: user exists")
else:
    print("Unknown error")
'''

'''
result = db.Sec_UserDelete('userTest5')
if result == 1:
    print("user deleted successfully")
elif result == 0:
    print("Can NOT delete user: current user is not the owner")
elif result == -1:
    print("Can NOT delete user: Owner cannot delete himself")
elif result == -2:    
    print("Can NOT delete user: user does not exist")
else:
    print("Unknown error")
'''

'''
print("try to add group")
result = db.Sec_GroupAdd("UserGroup3")
if result == 1:
    print("Group created successfully")
elif result == 0:
       print("Can NOT create group: current user is not the owner")
elif result == -1:
    print("Group already exists")
else:
    print("Unknown error")
'''

'''
result = db.Sec_GroupDelete("UserGroup3")
if result == 1:
    print("Group deleted successfully")
elif result == 0:
       print("Can NOT delete group: current user is not the owner")
elif result == -1:
    print("Group does not exists")
else:
    print("Unknown error")
'''

'''
print("try to add user to group")
result = db.Sec_GroupAddUser("UserGroup4","userTest7")
if result == 1:
    print("User added to Group successfully")
elif result == 0:
       print("Can NOT add user to group: current user is not the owner")
elif result == -1:
    print("Group does not exists")
elif result == -2:
    print("User does not exists")
elif result == -3:
    print("User alredy belongs to this Group")
else:
    print("Unknown error")
'''

'''
result = db.Sec_GroupRemoveUser("UserGroup4","userTest6")
if result == 1:
    print("User removed from Group successfully")
elif result == 0:
       print("Can NOT removed user from group: current user is not the owner")
elif result == -1:
    print("Group does not exists")
elif result == -2:
    print("User does not exists")
elif result == -3:
    print("User does not belong to this Group")
else:
    print("Unknown error")
'''

'''
Permission.AccessGranted
Permission.AccessNotGranted
Permission.AccessDenied
'''
'''
print("try to grant permission to user")
result = db.Sec_UserGrantAccess("dbo","test1", Permission.AccessGranted, Permission.AccessGranted, Permission.AccessGranted, Permission.AccessGranted)
if result == 1:
    print("access granted successfully")
elif result == 0:
       print("Can NOT grant access: current user is not the owner")
elif result == -1:
    print("User does not exists")
elif result == -2:
    print("Table does not exists")
elif result == -3:
    print("permission exists")
else:
    print("Unknown error")
'''

'''
print("try to grant permission to group")
groupName = "UserGroup4"
result = db.Sec_GroupGrantAccess(groupName,"test1", Permission.AccessNotGranted, Permission.AccessDenied, Permission.AccessNotGranted, Permission.AccessNotGranted)
if result == 1:
    print("access granted successfully")
elif result == 0:
       print("Can NOT grant access: current user is not the owner")
elif result == -1:
    print(f"Group {groupName} does not exists")
elif result == -2:
    print("Table does not exists")
elif result == -3:
    print("permission exists")
else:
    print("Unknown error")
'''

'''
username = 'userTest1'
tablename = 'test1'
canRead , canInsert, canUpdate, canDelete = db.Sec_UserHasAccessToTable(username, tablename)
print(f"{username} to table {tablename} permissions canRead = [{canRead}] , canInsert = [{canInsert}], canUpdate = [{canUpdate}], canDelete = [{canDelete}]")
'''

'''
result = db.Sec_UserRevokeAccess("userTest7","test1")
if result == 1:
    print("access granted successfully")
elif result == 0:
       print("Can NOT grant access: current user is not the owner")
elif result == -1:
    print("User does not exists")
elif result == -2:
    print("Table does not exists")
elif result == -3:
    print("user does not have persmissions")
else:
    print("Sec_UserRevokeAccess Unknown error")
'''


db.select('meta_length', '*')
db.select('meta_users', '*')
db.select('meta_userGroups', '*')
db.select('meta_userGroups_Users', '*')
db.select('meta_persmissions', '*')
#db.select('meta_persmissions',['GUID','table_name','canRead','canInsert','canUpdate','canDelete'])

#db.drop_db()
