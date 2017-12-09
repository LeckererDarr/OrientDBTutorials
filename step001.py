import pyorient

id = "your id"
pw = "your pw"

def showResult(results):
    for r in results:
        print(r)

# 클라이언트 초기화
client = pyorient.OrientDB("localhost", 2424)
session_id = client.connect(id, pw)

# DB 목록 보기
print(client.db_list().databases)
# {'o2': 'plocal:/Users/darr/OrientDB/databases/o2', 'pytest': 'plocal:/Users/darr/OrientDB/databases/pytest', 'GratefulDeadConcerts': 'plocal:/Users/darr/OrientDB/databases/GratefulDeadConcerts', 'CorpusTest': 'plocal:/Users/darr/OrientDB/databases/CorpusTest'}

# DB 열기
client.db_open("pytest", id, pw)

# select
results = client.command('select * from test1')
showResult(results)
#{'@test1':{'name': '김철수'},'version':1,'rid':'#26:0'}
#{'@test1':{'name': '김영희', 'age': 23},'version':1,'rid':'#27:0'}

print(type(results[0].oRecordData))
print(results[0]._rid) # #26:0
print(results[0]._version) # 1
print(results[0].oRecordData) # {'name': '김철수'}

#insert
#client.command("insert into test1(name, addr) values('%s','%s')"%('박근혜','503'))

print()
results = client.command('select * from test1 where')
showResult(results)
#{'@test1':{'name': '김철수'},'version':1,'rid':'#26:0'}
#{'@test1':{'name': '박근혜', 'addr': '503'},'version':1,'rid':'#26:1'}
#{'@test1':{'name': '김영희', 'age': 23},'version':1,'rid':'#27:0'}

client.close()