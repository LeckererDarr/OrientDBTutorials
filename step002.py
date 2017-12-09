# I did change this, go to step003.py!
import pyorient

class MyError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class OrientHandler:
    # 초기값
    __id = "your id"
    __pw = "your pw"
    __host = "localhost"
    __port = 2424

    # 생성자
    def __init__(self):
        pass

    # 연결 정보 설정
    def setConnInfo(self, id, pw, host, port):
        self.__id = id; self.__pw = pw; self.__host = host; self.__port = port;

    # 커넥션 생성
    def createConn(self):
        client = pyorient.OrientDB(self.__host, self.__port)
        session_id = client.connect(self.__id, self.__pw)

        return client, session_id

    # 데이터베이스 열기
    def openDB(self, client, db_name):
        # Call by ref로 return할 필요는 없습니다
        client.db_open(db_name, self.__id, self.__pw)

    # 연결종료
    def close(self, client):
        client.close()

    # where문 조립
    def __assembleWhere(self, where):
        w = ''
        for i, k_v in enumerate(where.items()):
            w += ' %s = "%s" '%k_v
            if(i<len(where)-1):
                w += 'and'
        return w

    # Create
    def create(self, client, class_name, values):
        try:
            if values:
                client.command('insert into %s content %s'%(class_name, values))
            else:
                raise MyError("please check parameter : values")
        except Exception as e:
            print('[error : create]',e)

    # Read
    def read(self, client, class_name, where):
        results = []
        try:
            if where:
                w = self.__assembleWhere(where)
                results = client.command('select * from %s where %s'%(class_name, w))
            else:
                results = client.command('select * from %s'%class_name)
        except Exception as e:
            print('[error : read]',e)
        finally:
            return results



    # Update
    def update(self, client, class_name, set, where):
        try:
            if set and where: # 조건절이 있을때
                w = self.__assembleWhere(where)
                client.command('update %s merge %s where %s'%(class_name, set, w))
                pass
            elif set and not where: # 조건절이 없을때
                client.command('update %s merge %s' % (class_name, set))
            else:
                raise MyError("please check parameter : set")
        except Exception as e:
            print('[error : update]', e)


    # Delete
    # where 인자가 없으면 클래스 내용이 전부 제거됩니다
    def delete(self, client, class_name, where):
        try:
            if where:
                w = self.__assembleWhere(where)
                client.command('delete from %s where %s'%(class_name, w))
            else:
                client.command('delete from %s'%class_name)
        except Exception as e:
            print('[error : delete]', e)


    # query_async
    def async(self, client, class_name):
        query = 'select * from %s'%class_name
        print(query)
        client.query_async(query, -1, "*:-1", self.__callback)

    def __callback(self, record):
        print(record)



## end of CrudClass

def showResult(results):
    for r in results:
        print(r)
    print()

if __name__=="__main__":
    orientCRUD = OrientHandler()

    # 커넥션 생성
    client, session_id = orientCRUD.createConn()
    # 데이터베이스 열기
    orientCRUD.openDB(client, 'pytest')

    # create
    orientCRUD.create(client, 'test1', {'name':'안기모'})


    # read
    results = orientCRUD.read(client, 'test1', None)
    showResult(results)
    #{'@test1': {'name': '김철수'}, 'version': 1, 'rid': '#26:0'}
    #{'@test1': {'name': '박근혜', 'addr': '503'}, 'version': 1, 'rid': '#26:1'}
    #{'@test1': {'name': '김영희', 'age': 23}, 'version': 1, 'rid': '#27:0'}
    #{'@test1': {'name': '안기모'}, 'version': 1, 'rid': '#28:0'}


    # update
    orientCRUD.update(client, 'test1', {'age':90}, {'name':'안기모'})
    orientCRUD.update(client, 'test1', {'nation': '대한민국'}, None) # 조건절이 없으면 모든 데이터에 적용됩니

    results = orientCRUD.read(client, 'test1', {'age':90})
    showResult(results)
    #{'@test1':{'name': '안기모', 'age': 90, 'nation': '대한민국'},'version':2,'rid':'#28:0'}


    # delete
    orientCRUD.delete(client, 'test1', {'name': '안기모'})

    # close
    orientCRUD.close(client)



    # query_async
    client, session_id = orientCRUD.createConn()
    orientCRUD.openDB(client, 'CorpusTest')
    orientCRUD.async(client, 'noun_key')
