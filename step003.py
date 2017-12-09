# step002 version2

import pyorient

class MyError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class OrientHandler:
    # 선언
    __id = None
    __pw = None
    __host = None
    __port = None
    __db_name = None

    __client = None
    __session_id = None

    # 생성자
    def __init__(self, id, pw, host, port, db_name):
        self.setConnInfo(id, pw, host, port, db_name)
        self.createConn()
        self.openDB()

    # 연결 정보 설정
    def setConnInfo(self, id, pw, host, port, db_name):
        self.__id = id; self.__pw = pw; self.__host = host; self.__port = port; self.__db_name=db_name

    # 커넥션 생성
    def createConn(self):
        self.__client = pyorient.OrientDB(self.__host, self.__port)
        self.__session_id = self.__client.connect(self.__id, self.__pw)

    # 데이터베이스 열기
    def openDB(self):
        # Call by ref로 return할 필요는 없습니다
        self.__client.db_open(self.__db_name, self.__id, self.__pw)

    # 연결종료
    def close(self):
        self.__client.close()

    # where문 조립
    def __assembleWhere(self, where):
        w = ''
        for i, k_v in enumerate(where.items()):
            w += ' %s = "%s" '%k_v
            if(i<len(where)-1):
                w += 'and'
        return w

    # Create
    def create(self, class_name, values):
        try:
            if values:
                self.__client.command('insert into %s content %s'%(class_name, values))
            else:
                raise MyError("please check parameter : values")
        except Exception as e:
            print('[error : create]',e)

    # Read
    def read(self, class_name, where):
        results = []
        try:
            if where:
                w = self.__assembleWhere(where)
                results = self.__client.command('select * from %s where %s'%(class_name, w))
            else:
                results = self.__client.command('select * from %s'%class_name)
        except Exception as e:
            print('[error : read]',e)
        finally:
            return results

    # Update
    def update(self, class_name, set, where):
        try:
            if set and where: # 조건절이 있을때
                w = self.__assembleWhere(where)
                self.__client.command('update %s merge %s where %s'%(class_name, set, w))
            elif set and not where: # 조건절이 없을때
                self.__client.command('update %s merge %s' % (class_name, set))
            else:
                raise MyError("please check parameter : set")
        except Exception as e:
            print('[error : update]', e)

    # Delete
    # where 인자가 없으면 클래스 내용이 전부 제거됩니다
    def delete(self, class_name, where):
        try:
            if where:
                w = self.__assembleWhere(where)
                self.__client.command('delete from %s where %s'%(class_name, w))
            else:
                self.__client.command('delete from %s'%class_name)
        except Exception as e:
            print('[error : delete]', e)


    # query_async
    def async(self, class_name):
        query = 'select * from %s'%class_name
        self.__client.query_async(query, -1, "*:-1", self.__callback)

    def __callback(self, record):
        print(record)



## end of CrudClass

def showResult(results):
    for r in results:
        print(r)
    print()

if __name__=="__main__":
    id = "your id"
    pw = "your pw"
    host = "localhost"
    port = 2424

    pytest = OrientHandler(id, pw, host, port, 'pytest')

    # create
    pytest.create('test1', {'name': '안기모'})

    # read
    results = pytest.read('test1', None)
    showResult(results)
    #{'@test1': {'name': '김철수'}, 'version': 1, 'rid': '#26:0'}
    #{'@test1': {'name': '박근혜', 'addr': '503'}, 'version': 1, 'rid': '#26:1'}
    #{'@test1': {'name': '김영희', 'age': 23}, 'version': 1, 'rid': '#27:0'}
    #{'@test1': {'name': '안기모'}, 'version': 1, 'rid': '#28:0'}


    # update
    pytest.update('test1', {'age':90}, {'name':'안기모'})
    pytest.update('test1', {'nation': '대한민국'}, None) # 조건절이 없으면 모든 데이터에 적용됩니

    results = pytest.read('test1', {'age':90})
    showResult(results)
    #{'@test1':{'name': '안기모', 'age': 90, 'nation': '대한민국'},'version':2,'rid':'#28:0'}


    # delete
    pytest.delete('test1', {'name': '안기모'})

    # close
    pytest.close() # 재사용하려면 pytest.createConn(); pytest.openDB(); 후 사용



    # query_async
    # CorpusTest라는 데이터베이스는 제가 임의로 만든 데이터베이스입니다
    # 'CorpusTest'와 'noun_key'만 여러분의 데이터베이스와 클래스 이름으로 변경하면 query_async를 사용하실 수 있습니다
    corpusTest = OrientHandler(id, pw, host, port, 'CorpusTest')
    corpusTest.async('noun_key')