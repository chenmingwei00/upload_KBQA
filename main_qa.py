#! -*- coding:utf-8 -*-
import math
import pickle
import jieba
import gensim
import pandas as pd
import jieba.analyse
import jieba.posseg
# from KBQA_small_data_version1.simaple_kbqa_1.hellp_ltp import *
from stanfordcorenlp import StanfordCoreNLP
from KBQA_small_data_version1.kbqa.connectSQLServer import connectSQL
from KBQA_small_data_version1.kbqa.entity_recognize import Entity
import numpy as np
import re
from IPython.display import display
# from simaple_kbqa_1.hellp_ltp import *
# host = '172.17.0.169'
host = '172.16.211.128'

user = 'sa'
password = 'chentian184616_'
database= 'chentian'
querySQL = connectSQL(host, user, password, database)
pd.set_option('display.max_columns',5000)
pd.set_option('display.max_rows',5000)
pd.set_option('display.width',1000000)
pd.set_option('display.max_columns',None)
class Robots:
    def __init__(self):
        pkl_file = open('./KBQA_small_data_version1/train_param/entity_template.pkl', 'rb')
        self.template_property = pickle.load(pkl_file)
        ppt_file=open('./KBQA_small_data_version1/train_param/ppt_update_update1.pkl', 'rb')
        self.ppt_property=pickle.load(ppt_file)
        concept_fre=open('./KBQA_small_data_version1/train_param/concept_count.pkl', 'rb')
        self.concept_fre=pickle.load(concept_fre)
        self.jieba_pos = ['i', 'j', 'l','nr', 'nt', 'nz', 'b', 'nrfg','zg']
        self.unused_pos=['b','c','dg','e','o','p','r','u','w','y','z','uj','x']
        self.stanford_pos=['NR']
        self.tf_idf = jieba.analyse.extract_tags
        self.nlp = StanfordCoreNLP(path_or_host='./KBQA_small_data_version1/stanford-corenlp/stanford-corenlp-full-2017-06-09/',lang='zh')
        self.sql2 = "SELECT * FROM [chentian].[dbo].[baike_triples1] WHERE entity ='%s' "
        self.sql = "SELECT * FROM [chentian].[dbo].[baike_triples1] WHERE entity in %(name)s "
        self.sq3="SELECT * FROM [chentian].[dbo].[m2e1] where entity='%s'"
        self.entity_re=Entity()
        self.model = gensim.models.Word2Vec.load('./KBQA_small_data_version1/word2vec/corpus.model')
    def get_answer_qa(self,sentence):
        """
        对用户问题进行实体识别，产生实体，然后找到实体类别，形成template，
        匹配对应template库寻找对应属性答案
        :return:
        """
        final_result = []
        final_result_final = []
        second_result = []
        question_template = []
        # tempplate_sort={}#对模板进行排序
        template_property={}#模板对应属性，属性已经排序成功
        # entities=self.syntactic_entity(sentence)
        # entities = self.syntactic_entity(sentence)
        # print(entities,"$$$$$$$$$$$$$$$$$$$$$$")
        # if len(entities) == 0:
        entities = self.entity_recognize(sentence)
        # entities = list(set(entities))
        print(entities,"#######################################################")
        # sentence.replace("《","").replace("》","").replace("“","").replace("”","").replace("‘","").replace("’","")
        for entity in entities:
            # print(entity,"^^^^^^^^^^^^^^^^^6666666666666666666666666666666")
            entity = entity.replace("'", "''")
            real_entity = [k.replace("'", "") for k in
                           self.entity_re.get_synonym1(entity)['real_entities']]  # 由于实体中可能包含',则替换为'' 在数据库中就认为是单引号
            if len(real_entity) == 0:
                real_entity = "('" + str(entity) + "')"  # 如果m2e文件中没有多义词，则实体自己为real_entity
            elif len(real_entity) == 1:
                real_entity = "('" + str(real_entity[0]) + "')"
            else:
                real_entity = tuple(real_entity)
            # real_entity=self.get_synonym2(entity)
            # print(real_entity)
            temp_sql = self.sql % {'name': real_entity}  # real_entity 是一个元组，
            result = querySQL.Query(temp_sql)  # 用sqlserver的in (e1,e2,e3)元组中得到所有的结果，不用再对real_entity实体循环多次select查找
            # print(result)
            result['template_score']=''
            result['property_score']=''
            result['score']=''
            concepts = result[result['property'] == 'BaiduTAG']['value']
            for pro in concepts:
                temp_template = sentence.replace(entity, '$$$$$' + pro + "$$$$$")  # 对应concept形成问题模板
                # print("tempplte", temp_template)
                if temp_template in self.template_property:
                    # tempplate_sort[temp_template]=self.concept_fre[pro]#把对应的概念频数赋值给对应的模板
                    predicts = self.template_property[temp_template]#模板对应的属性
                    # print(list(set(predicts)))
                    property_fre = self.ppt_property[temp_template]
                    property_fre=dict(sorted(property_fre.items(), key=lambda d: d[1], reverse=True)[:4])
                    template_property[temp_template]=property_fre
                    # print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
                    for predict in list(property_fre.keys()):
                        if predict=="BaiduTAG":continue
                        if predict=='BaiduCARD':final_result_final.append(result[result['property']=='BaiduCARD'])
                        elif len(result[result['property']==predict])!=0:
                            # print(property,result[result['property']==predict],"333333333333333333333333333333333333333333")
                            result.loc[result['property']==predict,['template_score']]=self.concept_fre[pro]#把对应模板的分数赋值，为模板排序做准备
                            result.loc[result['property'] == predict, ['property_score']]=property_fre[predict]
                            result.loc[result['property'] == predict, ['score']]=self.concept_fre[pro]*property_fre[predict]
                            final_result.append(result[result['property']==predict])
                second_result.append(result)
                # print(result,"&&&&&&&&&&&&&&&&&&&&&&&&&&")
        # print(final_result_final,"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$4")
        if len(final_result)!=0:
            print("11111111111111111111111111111111111111111111111111111111111")
            if len(final_result)!=0:
                final_result=pd.concat(final_result).drop_duplicates()
                tempresult=''.join(list(final_result.sort_values(by=['score'], ascending=False).loc[:,['entity','property', 'value']].iloc[0]))
                return tempresult
            else:return 'no_answer'
            # return self.sort_result(final_result)
        elif len(final_result_final)!=0:
            print("222222222222222222222222222222222222222222222222222222222222")
            if len(final_result_final)!=0:
                final_result_final=pd.concat(final_result_final).drop_duplicates()
                return self.sort_result(final_result_final,sentence)
            else:return 'no_answer'
        else:
            print("33333333333333333333333333333333333333333333333333333333333")
            if len(second_result)!=0:
                final_result= pd.concat(second_result).drop_duplicates()
                final_result=list(self.sort_result(final_result,sentence).reset_index().loc[0])[1:]
                return ''.join(final_result[:2])+"："+final_result[-1].replace("<a>",'').replace('</a>','')
            else:return 'no_answer'
    def sort_result(self,data_fream,sentence):
        """
        对最后结果按照热度进行排序
        :param data_fream: 输入数据
        :return:
        """
        entities = data_fream['entity']
        entities=list(set(entities))
        if len(entities)>=1:
            data_fream['score'] = ''
            data_fream['property_score']=''
            data_fream['cos_score']=''
            for ele in entities:
                if len(ele.split("（"))>1:
                    ele_temp=ele.split("（")[1].replace('）',"")
                    entity=ele.split("（")[0] #表示问句中的实体
                    important_words = self.tf_idf(ele_temp)
                    important_words = important_words[:math.ceil(len(important_words) * 0.8)]
                    scorce = 0
                    for word in important_words:
                        if word==entity:continue #如果修饰词中含有问句的实体，则不计为相似词 2017/12/27
                        try:
                            scorce += self.model.similarity(entity, word)
                        except:
                            scorce = 0
                    data_fream.loc[data_fream['entity'] == ele, ['score']] = scorce
                    property_word = []
                    rest_words = sentence.replace(entity, '')
                    pos_words=jieba.posseg.cut(rest_words)
                    # print(pos_words)
                    for i in pos_words:
                        # print(i.word,i.flag)
                        if i.flag not in self.unused_pos:
                            property_word.append(i.word)
                    properties=list(data_fream['property'])
                    # print(property_word,"8888888888888888888888888888888888888888888888888888")
                    for pro in properties:
                        ask_vec=np.zeros(400);query_vec=np.zeros(400)
                        pro_words='|'.join(jieba.cut(pro)).split("|")
                        for wor in pro_words:
                            try:
                                ask_vec+=self.model[wor]
                            except:continue
                        # print(property_word)
                        for wor1 in property_word:
                            try:
                                query_vec+=self.model[wor1]
                            except:continue
                        cos_simil = self.cosSimil(ask_vec, query_vec)  # +perSimil
                        # if cos_simil!=0:
                        #     print(cos_simil,"7777777777777777777777777777777777777777777777777777")
                        data_fream.loc[(data_fream['entity']==ele)&(data_fream['property']==pro),['cos_score']]=cos_simil
                else:
                    property_word = []
                    rest_words = sentence.replace(ele, '')
                    pos_words = jieba.posseg.cut(rest_words)
                    for i in pos_words:
                        # print(i.word,i.flag)
                        if i.flag not in self.unused_pos:
                            property_word.append(i.word)
                    properties = list(data_fream['property'])
                    for pro in properties:
                        ask_vec = np.zeros(400);
                        query_vec = np.zeros(400)
                        pro_words = '|'.join(jieba.cut(pro)).split("|")
                        for wor in pro_words:
                            try:
                                ask_vec += self.model[wor]
                            except:
                                continue
                        for wor1 in property_word:
                            try:
                                query_vec += self.model[wor1]
                            except:
                                continue
                        cos_simil = self.cosSimil(ask_vec, query_vec)  # +perSimil
                        data_fream.loc[
                            (data_fream['entity'] == ele) & (data_fream['property'] == pro), ['cos_score']] = cos_simil
            fin_data=[]
            arclen=math.ceil(len(data_fream)*0.3)
            fin_data.append(data_fream.sort_values(by='cos_score',ascending=False)[:arclen]) #后是属性排序)
            fin_data.append(data_fream.sort_values(by='score',ascending=False)[:arclen])
            return pd.concat(fin_data).loc[:,['entity','property','value']]
        else:
            return data_fream.loc[:,['entity','property','value']]
            # entity_score[ele]=scorce
        # entity_score=dict(sorted(entity_score.items(),key=lambda d:d[1] ,reverse=True))
        # 计算余弦相似度
    def cosSimil(self, v1, v2):
        return np.dot(v1, v2) / (
        math.sqrt(sum(v1 ** 2)) * math.sqrt(sum(v2 ** 2)) + 0.000000000000000000000000000000001)
    def entity_recognize(self,sentence):
        """
        识别出问题中对应的实体
        :param sentence: 用户问题
        :return: 返回实体
        """
        if re.search('《.*》', sentence)!=None :
            return [re.search('《.*》', sentence).group().replace("《", "").replace("》", "")]
        if re.search('“.*”', sentence) :
            return [re.search('“.*”', sentence).group().replace("“", "").replace("”", "")]
        if re.search('‘.*’', sentence):
            return [re.search('‘.*’', sentence).group().replace("‘", "").replace("’", "")]
        jieba_cut = "|".join(jieba.cut(sentence)).split("|")
        if "是谁唱的" in sentence or "是谁写的" in sentence or "谁唱" in sentence or "谁写" in sentence:
            question_entity = ''
            for e in sentence:
                if e == "是" or e == "谁": break
                question_entity += e
            question_entity = [question_entity]
        else:
            question_entity = self.nlp.ner(sentence)  # 获得Stanford的实体识别的结果，以及切词结
            print(question_entity,"222222222222222222222222222222")

            pos_jieba = jieba.posseg.cut(sentence)
            tf_idf = jieba.analyse.extract_tags
            JIE = tf_idf(sentence)
            if len(jieba_cut) < len(question_entity):#如果结巴切词比Stanford少，
                final_words = []
                for ele in jieba_cut:
                    tem_word = ''
                    flag = False
                    for el in question_entity:
                        if el[0] in ele:
                            if el[1] != 'O' and el[1] != 'NT' and el[1] != 'NUMBER': flag = True
                            tem_word += el[0]
                    if flag == True:
                        final_words.append(tem_word)
                question_entity = final_words
                # print(question_entity,"^^^^^^^^^^^^^^^^^^^^^^^^")
            else:
                question_entity = self.entity_re.entity_connect(question_entity)
                # print(question_entity,"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@2")
            if len(question_entity)==0:
                stanford_pos = self.nlp.pos_tag(sentence)
                for wor in stanford_pos:
                    if wor[1] in self.stanford_pos:
                        question_entity=[wor[0]]
            if len(question_entity) == 0:
                for i in pos_jieba:
                    # print(i.word, i.flag, "#################################################")
                    if i.flag in self.jieba_pos:
                        question_entity.append(i.word)
            # print(question_entity, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")
            # #对实体进行连接，相邻作为一个实体在kb中寻找，依次递减
            # 如果整个句子中不包含实体，则需要从m2e中寻找且此后对应的实体,从名词‘NN’中作为备选实体
            if len(question_entity) == 0:
                jieba_entity = []
                jieba_pos = jieba.posseg.cut(sentence)
                for i in jieba_pos:
                    # print(i,"&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
                    if i.flag in self.jieba_pos:
                        jieba_entity.append(i.word)
                question_entity = jieba_entity
            # print(question_entity,"###################################################")
            if len(question_entity) == 0:
                # print(JIE)
                words_tag_jieba = JIE[:math.ceil(len(JIE) * 0.3)]  # 这是jieba切词结果，要比stanford更符合中文习惯，
                question_entities = []
                try:
                    words_tag = self.nlp.pos_tag("".join(words_tag_jieba))
                    # print(len(words_tag_jieba) , len(words_tag))
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
                            if value[1] in self.stanford_pos:
                                question_entities.append(value[0])
                        question_entity = question_entities
                        # print(question_entity,"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$4")
                except:
                    # print(sentence, "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$44")
                    return 0
        if len(question_entity)==0:
            tf_idf = jieba.analyse.extract_tags
            JIE = tf_idf(sentence)
            if len(JIE)==0:JIE=[sentence]
            # print(JIE[0],"$$$$$$$$$$$$$$$$$$")
            # print(question_entity,"**************")
            extract = {}  # 提取出问题中的实体以及答案中的value,还有对应的property ,类型为[entity,property,value]
            question_entity.append(JIE[0])
        # question_entity = self.connect_entity(jieba_cut, question_entity)
        # print(question_entity, "**************")
        return question_entity
    def connect_entity(self,question,question_entity):
        prio = []
        real_enity=[]
        for question_e in question_entity:
            if question_e in question:
                prio.append(question.index(question_e))
        k=1
        # print(question_entity)
        while k<len(prio):
            if prio[k]-prio[k-1]==1:
               temp_enity=question[prio[k-1]]+question[prio[k]]
               # print(question[prio[k-1]])
               # print(question[prio[k]])
               # print(question_entity,"^^^^^^^^^^^")
               if question[prio[k-1]] in question_entity:
                   question_entity.remove(question[prio[k-1]])
               if question[prio[k]] in question_entity:
                   question_entity.remove(question[prio[k]])
               real_enity.append(temp_enity)
            k+=1
        real_enity.extend(question_entity)
        return real_enity
    def syntactic_entity(self,sentence):
        """
         通过句法分析获取实体，按照树结构的和中心动词切分
        :param sentence:
        :return:
        """
        tree, jieba_ext = dependency(sentence)
        entity_list = get_tree(tree, sentence)
        candiate_entities=sorted(dict(entity_list).items(),key=lambda d:d[1],reverse=True)
        for entity in candiate_entities:
            print(entity,"%%%%%%%%%%%%%%%%%%%%%%%")
            temp_sql = self.sq3 % entity[0]  # real_entity 是一个元组，
            result = querySQL.Query(temp_sql)  # 用sqlserver的in (e1,e2,e3)元组中得到所有的结果，不用再对real_entity实体循环多次select查找
            if len(result) != 0:
                return list(result['real_entities'])
            else:
                temp_sql = self.sql2 % entity[0] # real_entity 是一个元组，
                result = list(set(list(querySQL.Query(temp_sql)['entity'])))  # 用sqlserver的in (e1,e2,e3)元组中得到所有的结果，不用再对real_entity实体循环多次select查找
                if len(result)!=0:
                    return result
                else:continue
        return pd.DataFrame()


if __name__ == '__main__':
    robot = Robots()
    # sentence=u"带有辅助加热器的器具"
    # sentence="我想知道时间煮雨是谁唱的"
    # sentence="现在是什么时间"
    # sentence="中国国家主席"
    # sentence='小苹果是谁唱的'
    # sentence='小苹果的演唱者'
    # sentence="空调部件有哪些"
    # sentence="鞠躬尽瘁，死而后已是谁说的"
    # result = robot.syntactic_entity(sentence)
    # print(result)
    # sentence="手机发热怎么回事"
    sentence="格力电器总部在哪里"
    # sentence="格力电器口号是什么"
    resu=robot.get_answer_qa(sentence)
    print(resu,"%%%%%%%%%%")
    # i=0
    #
    # with open("E:/chenmingwei/KBQA_small_data/data/train_questions_with_evidence1.txt",encoding='utf-8') as f1:
    #     lines=f1.readlines()
    #     for line in lines:
    #         que=line.split("&&&&&")[0]
    #         print(que)
    #         result=robot.get_answer_qa(que)
    #         if len(result)!=0:
    #             print(result.head())
    #         else:
    #             print(result)
    #         print("111111111111111111111111111111111111111111111111111111111111111111")
    #         print("111111111111111111111111111111111111111111111111111111111111111111")
    #         print("111111111111111111111111111111111111111111111111111111111111111111")
    #         print("111111111111111111111111111111111111111111111111111111111111111111")
    #         if i>1000:break
    #         i+=1
    # sql="SELECT * FROM [chentian].[dbo].[m2e1]  where entity='张杰'"
    # result = list(querySQL.Query(sql)['real_entities'])
    # tf_idf=jieba.analyse.extract_tags

    #     print(scorce,"5555555555555555555")
    # data=pickle.load(open("E:\chenmingwei\KBQA_small_data\data\concept_count.pkl",'rb'))
    # for key,value in data.items():
    #     print(key,value)