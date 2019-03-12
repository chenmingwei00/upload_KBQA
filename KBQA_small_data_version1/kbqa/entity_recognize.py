#! -*- coding:utf-8 -*-

"""
为了识别问题与答案中的实体;数据保存在sqlserver，
思路一：首先加载m2e.txt实体到用户词典；对问题进行切词；（1）通过命名实体识别识别实体，识别不出或者识别出来实体的用m2e,搜索实体，
        根据答案以及实体在KB中寻找三元组存成（q:{e1,e2,...,en}） 以及(e1:[property,v]) 用的函数是 save_evc 保存
"""
import jieba.analyse
import math
from collections import Counter
import jieba.posseg
from time import time
from stanfordcorenlp import StanfordCoreNLP
from KBQA_small_data_version1.kbqa.connectSQLServer import connectSQL
import pickle
# jieba.load_userdict('./../data/user_dict.txt')
# host = 'DQ26-000018Z29'ls
# user = 'chen'
# password = '123456'
# host = '172.17.0.169'
host = '172.16.211.128'

user = 'sa'
password = 'chentian184616_'
database= 'chentian'

querySQL = connectSQL(host, user, password, database)
class Entity:
    def __init__(self):
        self.jieba_pos=['i','j','l' ,'m' ,'nr','nt','nz','b','nrfg']
        self.tf_idf=jieba.analyse.extract_tags
        self.nlp = StanfordCoreNLP(path_or_host='../../stanford-corenlp/stanford-corenlp-full-2017-06-09/',lang='zh')
        self.sql="SELECT * FROM [chentian].[dbo].[baike_triples1] WHERE entity in %(name)s "
        self.sql2="SELECT * FROM [chentian].[dbo].[baike_triples1] WHERE entity ='%s' "
        # self.question='D:/QA/answer.txt'
        self.sql1="SELECT real_entities FROM [chentian].[dbo].[m2e1] where entity='%s'"
        self.sql3="SELECT value FROM [chentian].[dbo].[baike_triples1] WHERE property='BaiduTAG' "
        # self.KB='./../data/baike_triples.txt'
        # self.m2e='./../data/m2e.txt'
    def name_entity(self,entity):
        """
        把实体对应的属性全部返回，包括对应类别
        :param entity:
        :return:
        """
        with open(self.KB,'r',encoding='utf-8') as f:
            lines=f.readlines()
            for line in lines:
                words=line.split("\t")
                if entity in words[0] :
                    print(line)
    def get_synonym(self,sentence):
        """
        获取实体对应的多义词
        :param entity:
        :return:
        """
        entiies=[]
        for line in open(self.m2e,'r',encoding='utf-8'):
            words=line.strip('\n').split("\t")
            if words[0] in sentence:
                entiies.append(words[1])
        return entiies

    def get_synonym2(self, entity):
        """
        获取实体对应的多义词
        :param entity:
        :return:
        """
        entiies = []
        for line in open(self.m2e, 'r', encoding='utf-8'):
            words = line.strip('\n').split("\t")
            if words[0] == entity:
                entiies.append(words[1])
        return entiies
    def get_synonym1(self,entity):
        """
        获取实体对应的多义词
        :param entity:
        :return:
        """
        temp_sql = self.sql1 % entity
        result = querySQL.Query(temp_sql)
        return result
    def save_evc(self,sentence,answer):
        """
        存储实体value以及对应类别
        :return: 返回问题为{key1 :{e1,p1,v1}, {e2,p2,v2}} 的形式
        """
        jieba_cut = "|".join(jieba.cut(sentence)).split("|")
        if "是谁唱的" in sentence or "是谁写的" in sentence or "谁唱" in sentence or "谁写" in sentence:
            question_entity = ''
            for e in sentence:
                if e == "是" or e=="谁": break
                question_entity += e
            question_entity=[question_entity]
        else:
            question_entity=self.nlp.ner(sentence) #获得Stanford的实体识别的结果，以及切词结
            # pos_re=self.nlp.pos_tag(sentence)
            print(question_entity,"2222222222222222")
            pos_jieba=jieba.posseg.cut(sentence)
            # print(pos_re)
            # print(question_entity)
            # print(jieba_cut)
            if len(jieba_cut)<len(question_entity):
                final_words = []
                for ele in jieba_cut:
                    tem_word = ''
                    flag = False
                    for el in question_entity:
                        if el[0] in ele:
                            if el[1] !='O' and el[1]!='NT' and el[1]!='NUMBER': flag = True
                            tem_word += el[0]
                    if flag == True:
                        final_words.append(tem_word)
                question_entity=final_words
                # print(question_entity,"^^^^^^^^^^^^^^^^^^^^^^^^")
            else:
                question_entity=self.entity_connect(question_entity)
                # print(question_entity,"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@2")
            for i in pos_jieba:
                # print(i.word, i.flag, "#################################################")
                if i.flag in self.jieba_pos:
                    question_entity.append(i.word)
            # print(question_entity, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")
            # #对实体进行连接，相邻作为一个实体在kb中寻找，依次递减
            #如果整个句子中不包含实体，则需要从m2e中寻找且此后对应的实体,从名词‘NN’中作为备选实体
            if len(question_entity)==0:
                jieba_entity=[]
                jieba_pos = jieba.posseg.cut(sentence)
                for i in jieba_pos:
                    if i.flag in self.jieba_pos:
                        jieba_entity.append(i.word)
                question_entity=jieba_entity
                # print(question_entity,"###################################################")
            if len(question_entity)==0:
                tf_idf=jieba.analyse.extract_tags
                JIE=tf_idf(sentence)
                # print(JIE)
                words_tag_jieba=JIE[:math.ceil(len(JIE)*0.3)] #这是jieba切词结果，要比stanford更符合中文习惯，
                question_entities=[]
                try:
                    words_tag = self.nlp.pos_tag("".join(words_tag_jieba))
                    if len(words_tag_jieba) < len(words_tag):
                        final_words = []
                        for ele in words_tag_jieba:
                            tem_word = ''
                            for el in words_tag:
                                if el[0] in ele:
                                    tem_word += el[0]
                            final_words.append(tem_word)
                        question_entity = final_words
                    else:
                        for value in words_tag:
                            # print(value)
                            # if value[1] == 'NN'or value[1]=='NR':
                            question_entities.append(value[0])
                        question_entity=question_entities
                    # print(question_entity,"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$4")
                except:
                    print(sentence,"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$44")
                    return 0
        question_e={}
        tf_idf = jieba.analyse.extract_tags
        JIE = tf_idf(sentence)
        # print(JIE[:2])
        # print(question_entity,"**************")
        extract={} #提取出问题中的实体以及答案中的value,还有对应的property ,类型为[entity,property,value]
        question_entity.extend(JIE[:2])
        question_entity=self.connect_entity(jieba_cut,question_entity)
        # print(question_entity, "**************")
        for entity in question_entity:  #查找m2e文件把所有有关的实体全部找出
            # print(entity,"88888")
            temp_sql_origal = self.sql2 % entity  # real_entity 是一个元组，
            result_origal = querySQL.Query(temp_sql_origal)  # 用sqlserver的in (e1,e2,e3)元组中得到所有的结果，不用再对real_entity实体循环多次select查找
            if len(result_origal)!=0:
                values = result_origal['value']
                for index, va in enumerate(values):
                    # print(va, answer, va.replace("<a>", '').replace("</a>", '') in answer)
                    # print(va, answer, answer in va.replace("<a>", '').replace("</a>", ''))
                    # print(va, answer, self.simple_similar(va.replace("<a>", '').replace("</a>", ''), answer))
                    # print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                    # 对于搜索出来的实体有两个条件可以添加（e,p,v）一是kb被包含在答案中，或者两者简单相似度为0.9以上添加相似对
                    if va.replace("<a>", '').replace("</a>", '') in answer or answer in va.replace("<a>", '').replace(
                            "</a>", '') or self.simple_similar(va.replace("<a>", '').replace("</a>", ''), answer) > 0.8:
                        if ' '.join(list(result_origal.loc[index])) in extract:
                            extract['&&&&&'.join(list(result_origal.loc[index]))] += 1
                        else:
                            extract['&&&&&'.join(list(result_origal.loc[index]))] = 1
            entity=entity.replace("'","''")
            real_entity= [k.replace("'", "") for k in self.get_synonym1(entity)['real_entities']] #由于实体中可能包含',则替换为'' 在数据库中就认为是单引号
            if len(real_entity)==0:real_entity="('"+str(entity)+"')" #如果m2e文件中没有多义词，则实体自己为real_entity
            elif len(real_entity)==1:real_entity="('"+str(real_entity[0])+"')"
            else:real_entity=tuple(real_entity)
            # real_entity=self.get_synonym2(entity)
            temp_sql = self.sql % {'name':real_entity}  #real_entity 是一个元组，
            result = querySQL.Query(temp_sql) #用sqlserver的in (e1,e2,e3)元组中得到所有的结果，不用再对real_entity实体循环多次select查找
            values=result['value']
            for index,va in enumerate(values):
                # print(va,answer,va.replace("<a>",'').replace("</a>",'') in answer)
                # print(va,answer,answer in va.replace("<a>",'').replace("</a>",''))
                # print(va,answer,self.simple_similar(va.replace("<a>",'').replace("</a>",''),answer))
                # print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                #对于搜索出来的实体有两个条件可以添加（e,p,v）一是kb被包含在答案中，或者两者简单相似度为0.9以上添加相似对
                if va.replace("<a>",'').replace("</a>",'') in answer or answer in va.replace("<a>",'').replace("</a>",'') or self.simple_similar(va.replace("<a>",'').replace("</a>",''),answer)>0.8:
                    if ' '.join(list(result.loc[index])) in extract:
                        extract['&&&&&'.join(list(result.loc[index]))]+=1
                    else:
                        extract['&&&&&'.join(list(result.loc[index]))]=1

        if len(extract)!=0:
            question_e[sentence]=extract
            print(question_e)
            return question_e
        else:
            # print(sentence,"%%%%",answer)
            # print("&&&&&&&&&&&")
            return 0

    def connect_entity(self,question,question_entity):
        prio = []
        real_enity=[]
        for question_e in question_entity:
            if question_e in question:
                prio.append(question.index(question_e))
        k=1
        print(question_entity)
        while k<len(prio):
            if prio[k]-prio[k-1]==1:
               temp_enity=question[prio[k-1]]+question[prio[k]]
               print(question[prio[k-1]])
               print(question[prio[k]])
               print(question_entity,"^^^^^^^^^^^")
               question_entity.remove(question[prio[k-1]])
               question_entity.remove(question[prio[k]])
               real_enity.append(temp_enity)
            k+=1
        real_enity.extend(question_entity)
        return real_enity
    def entity_connect(self,entity,flag=['O','NUMBER']):
        """
        函数作用就是如果两个识别出来的实体相连就认为是一个，某则作为新的实体添加
        """
        entities = []  # 根据stanford找到所有问题中的实体
        temp_entity = ''
        for value in entity:
            if value[1] not in flag:
                temp_entity += value[0]
            else:
                if temp_entity != '':
                    entities.append(temp_entity)
                    temp_entity = ''
        if temp_entity != '':
            entities.append(temp_entity)
        return entities
    def simple_similar(self,answer, sent):
        """
        比较两个字符串含有共同字符的个数的比例
        :return: 返回比例
        """
        count = 0
        answer_len = len(answer)
        sent_len = len(sent)
        min_len = 0
        if answer_len < sent_len:
            min_len = answer_len
            for an in answer:
                if an in sent:
                    count += 1
        else:
            min_len = sent_len
            for an in sent:
                if an in answer:
                    count += 1
        return count * 1.0 / min_len
    def get_pevq(self):
        """
        这个函数是所有的主函数，把问题答案QA语料得到基于KB的EV对
        :return: 返回【{'奥巴马什么时候出生的': {'奥巴马（圣枪游侠） 其他名称 奥巴马': 1, '奥巴马（美国第44任总统） 出生日期 1961年8月4日': 1}}】
        这样的列表形式，以后得存储形式，在效率不足的情况下，在进行讨论
        """
        final_pevq=[]
        i=0
        with open('./../data/train_questions_with_evidence1.txt','r',encoding='utf-8') as f:
            lines=f.readlines()
            start = time()
            for line in lines:
                # print(line)
                question,answer=line.strip().replace("\t","").split("&&&&&")
                question_dict=self.save_evc(question,answer)
                if question_dict!=0:
                    final_pevq.append(question_dict)
                i+=1
                if i%100==0:
                    end=time()
                    print("消耗的时间为"+str(end-start)+"秒")
        output=open('./../data/pqev_final_update.pkl','wb')
        pickle.dump(final_pevq,output)
        output.close()
    def store_EV(self,file_path):
        """
        本函数的作用是把pqev_final.pkl的构造成类似于e:{v1:频数，v2：频数，...,}和v:{e1:频数,e2:频数,...}
        :param file_path: 对应的pqev_final.pkl路径
        """
        entities_values={}
        value_entity={}
        file_path=open(file_path,"rb")
        train_data=pickle.load(file_path)
        for que1 in train_data:
            evi = list(que1.values())[0]  # 问题中的所有（实体-属性-值）
            for key in evi.keys():
                value_temp={}
                entity_temp={}
                e, p, v = key.split("&&&&&")  # 接下来对每一个v 遍历每一个问题中所有的相同v,得到对应的实体e，并且记录实体出现的频数 实体e可能出现多次,对第一个概率没有影响，但是对第二个有影响，本来有结果，
                if e in entities_values:
                    if v!='':
                        if v in entities_values[e]:
                            entities_values[e][v]+=1
                        else:
                            entities_values[e][v]=1
                else:
                    if v!='':
                        value_temp[v]=1
                        entities_values[e]=value_temp
                if v!='':
                    if v in value_entity:
                        if e !='':
                            if e in value_entity[v]:
                                value_entity[v][e]+=1
                            else:
                                value_entity[v][e]=1
                    else:
                        if e!='':
                            entity_temp[e]=1
                            value_entity[v]=entity_temp
        output = open('./../data/EV_two.pkl', 'wb')
        pickle.dump(entities_values, output)
        pickle.dump(value_entity,output)
        output.close()
        file_path.close()
    def get_baiduTag(self):
        """
        此函数是获取到concept ，并且计数每一个概念的频数作为概念的权重
        :return:
        """
        tags = querySQL.Query(self.sql3)  # 用sqlserver的in (e
        print(list(tags['value'])[:20])
        concept_count=Counter(list(tags['value']))
        concept_count=dict(concept_count)
        output = open('./../data/concept_count.pkl', 'wb')
        pickle.dump(concept_count, output)
        output.close()
if __name__=="__main__":
    # entity=Entity()
    # entity.get_baiduTag()
    # entity.store_EV("E:\chenmingwei\KBQA_small_data\data\pqev_final.pkl")
    # entity.get_pevq()
    EV=open("E:\chenmingwei\KBQA_small_data\data\pqev_final.pkl",'rb')
    entity_value=pickle.load(EV)
    for key in entity_value:
        print(key)
    # value_entity=pickle.load(EV)
    # for key,value in entity_value.items():
    #     print(key,value)
    # b='全面内战爆发后，国民党反动派在昆明杀害的民盟中央委员是： & & & & & 李公朴'

    # a='“昌黎先生”是？&&&&&韩愈'
    # que,ans=a.split("&&&&&")
    # print(len(ans))
    # result=entity.save_evc(que,ans)
    # print(result)
    # sentence='123广西贺州重大故意伤害案什么时候发生的'
    # words=' '.join(jieba.cut(sentence))
    # question = '奥巴马什么时候出生的'
    # answer = '奥巴马出生于1961年8月4日'
    # question='控制器原理'
    # answer='控制器（英文名称：controller）是指按照预定顺序改变主电路或控制电路的接线和改变<a>电路'
    # start1=datetime.datetime.now()
    # final_dict = entity.save_evc(question, answer)
    # print(final_dict)
    # result=entity.get_synonym1('蝴蝶')
    # result=tuple([k.replace("'",'"') for k in result['real_entities']])
    # temp_sql = entity.sql % {'name': result}  # real_entity 是一个元组，
    # print(temp_sql)
    # result = querySQL.Query(temp_sql)
    # print(result)
    # end1=datetime.datetime.now()
    # entiies=entity.get_synonym() #用于获取所有问题的实体，不进行切词处理，防止因为切词造成实体的丢失
                #对于答案，
    # for entit in entiies:
    #     entity.name_entity(entit,answer)


    # sql = "SELECT * FROM [chentian].[dbo].[baike_triples1] WHERE entity='%s'"% '"383"改革方案'
    # start1=datetime.datetime.now()
    # result=querySQL.Query(sql)
    # print(result)
    # end1=datetime.datetime.now()
    # print((end1 - start1).seconds)
    # print("***********************************************************")
    # start = datetime.datetime.now()
    # entity.name_entity('"383"改革方案')
    # end = datetime.datetime.now()
    # print((end - start).seconds)
    # con_count=pickle.load(open('./../data/concept_count.pkl','rb'))
    # print(con_count['歌手'])
    # for key,value in con_count.items():
    #     print(key,value)
    #     break