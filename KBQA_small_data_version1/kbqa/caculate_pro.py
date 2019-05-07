#! -*- coding:utf-8 -*-
import pickle
from connectSQLServer import connectSQL
host = '172.16.54.33'
user = 'sa'
password = 'chentian184616_'
database= 'chentian'
querySQL = connectSQL(host, user, password, database)
class Pro_onlines(object):
    def __init__(self,EV):
        self.EV=EV
        self.sql_current_evp="SELECT COUNT(*) FROM [chentian].[dbo].[baike_triples1] WHERE entity ='%s' AND property='%s'"
        self.sql_baidutag="SELECT value FROM [chentian].[dbo].[baike_triples1] WHERE entity ='%s' AND property='BaiduTAG'"
        # self.entity_values=entity_values
        # self.value_entities=value_entities
        self.concept_fre=pickle.load(open("./../data/concept_count.pkl",'rb'))
    def calculate_piq(self,que1):
        """
        计算当前问题qi的每一个实体的概率，可以认为是当前实体对当前问题的重要程度，计算了三个概率，并且这三个概率，都可以通过当前问题，进行计算，
        计算了p(e|q)的概率，计算时由于对EV对可能有多个重复实体记录，所以需要把分母进行累加计算，具体看代码56-57行，分子为所有实体额记录频数。
        第二个概率变化为，根据两篇论文，最终采用当前实体对应的类别的概率采用e:{c1:pre1,c2:pre2,...}的形式，
        第三个概率，论文中提到对于e，p的多个value采用均匀概率，并且唯一value概率为一。至此三个概率
        :param qi: 当前问题，以及对应的三元组形成的数据
        :return: 返回当前问题中每个实体对应p(e/q)已经求出
        """
        print(que1.keys(),"$$$$$$$$$$$$$$")
        evi=list(que1.values())[0]#问题中的所有（实体-属性-值）
        currente_pre1 = {} #当前问题的第一个概率p(e|qi)
        currente_pre2 = {}  # 是每一个实体对应value不同实体的频数
        current_pteq={}#对于问题模板的类别概率问题  e_c = {}  # 保存每一个实体对应的类别概率e:{c1:pre1,c2:pre2,...}
        current_pvep={}#对于当前问题的实体意图对应的value值得概率
        for key in evi.keys():
            e_c_pre = {}  # 当前问题每一个实体e对应的类别c的频数。
            e,p,v=key.split("&&&&&")#接下来对每一个v 遍历每一个问题中所有的相同v,得到对应的实体e，并且记录实体出现的频数 实体e可能出现多次,对第一个概率没有影响，但是对第二个有影响，本来有结果，
            #                         重复第二次没有对应baidutag，则会重新赋值为空，
            if v!='' and p!='' and v!='':
                current_e = 0  # 当前实体对应的频数 分子
                current_alle = 0  # 对当前value的不同实体记总数 分母
                entity_value_temp=self.entity_values[e]#得到对应实体的value以及频数
                value_entity_temp=self.value_entities[v]#得到对应value的
                for entity_key,entity_pre in entity_value_temp.items():
                    if entity_key==v:
                        current_e=entity_pre
                        current_alle=sum(list(value_entity_temp.values()))
                        currente_pre1[e]=float(current_e)/float(current_alle)
        print(currente_pre1)
            # current_pvep_pre=querySQL.Query(self.sql_current_evp%(e,p))[''][0] #计算同一实体e同一意图p的不同值v的个数

        #     current_pe=0 #当前实体，对应类别（p,e）共同满足的个数
        #     current_allp=0 #当前实体的频数在整个EV中，作为求类别的分母。
        #     for que_ev in self.EV: #整个for循环就把所有的实体遍历所有问题
        #         current_evi=list(que_ev.values())[0] #当前EV当前问题的所有实体对
        #         for key1 in current_evi.keys():  #对于每一个实体对
        #             e1,p1,v1=key1.split("&&&&&")
        #             if v ==v1: #如果value相同
        #                 current_alle+=1 #对应实体的value其他共有多少实体的频数
        #                 if e1==e:current_e+=1 #如果并且是与当前问题实体相同，当前实体加一
        #             if e1==e:#对于当前类别的个数 分子，由于实体e可能出现多次，对于第一个计算只是重复，并没有影响
        #                 current_allp+=1
        #                 if p1.lower()=='baidutag':#并且是当前实体的类别
        #                     if v1 not in e_c_pre:e_c_pre[v1]=1
        #                     else:e_c_pre[v1]+=1
        #                     current_pe+=1#并且是当前实体对应的分类标
        #     if e not in current_pteq:
        #         for key_b,value in e_c_pre.items():
        #             e_c_pre[key_b]=float(value)/float(current_allp)
        #         current_pteq[e]=e_c_pre
        #     currente_pre1[e]=float(current_allp) #对应第一个分子
        #     if e in currente_pre2: currente_pre2[e]+=float(current_alle) #把所有实体value对应实体的分母相加
        #     else: currente_pre2[e]=float(current_alle) #然后把相同实体的分母相加
        #
        #     current_pvep[key]=1.0/float(current_pvep_pre)#之所以分子为1，原因是，如果在KB中e,p一旦确定，对应的数值给定均匀概率，并且公式只有value在变，
        # for key_5,value5 in currente_pre1.items():
        #     currente_pre1[key_5]=float(value5)/float(currente_pre2[key_5])
        # return currente_pre1,current_pteq,current_pvep
    def calculate_piq_kb(self,que1):
        """
        计算当前问题qi的每一个实体的概率，可以认为是当前实体对当前问题的重要程度，计算了三个概率，并且这三个概率，都可以通过当前问题，进行计算，
        计算了p(e|q)的概率，计算时由于对EV对可能有多个重复实体记录，所以需要把分母进行累加计算，具体看代码56-57行，分子为所有实体额记录频数。
        第二个概率变化为，根据两篇论文，最终采用当前实体对应的类别的概率采用e:{c1:pre1,c2:pre2,...}的形式，
        第三个概率，论文中提到对于e，p的多个value采用均匀概率，并且唯一value概率为一。至此三个概率
        :param qi: 当前问题，以及对应的三元组形成的数据
        :return: 返回当前问题中每个实体对应p(e/q)已经求出
        """
        # print(que1.keys(),"$$$$$$$$$$$$$$")
        evi=list(que1.values())[0]#问题中的所有（实体-属性-值）
        currente_pre1 = {} #是第一个实体的总数
        currente_pre2 = {}  # 是每一个实体对应value不同实体的频数
        current_pteq={}#对于问题模板的类别概率问题  e_c = {}  # 保存每一个实体对应的类别概率e:{c1:pre1,c2:pre2,...}
        current_pvep={}#对于当前问题的实体意图对应的value值得概率
        for key in evi.keys():
            e,p,v=key.split("&&&&&")#接下来对每一个v 遍历每一个问题中所有的相同v,得到对应的实体e，并且记录实体出现的频数 实体e可能出现多次,对第一个概率没有影响，但是对第二个有影响，本来有结果，
            #                         重复第二次没有对应baidutag，则会重新赋值为空，
            if e!='' and p!=''and v!='':
                temp_confre={}
                concept_fre=[]
                baidutag=list(querySQL.Query(self.sql_baidutag%e)['value'])
                for bai_temp in baidutag:
                    concept_fre.append(self.concept_fre[bai_temp])
                    # print(self.concept_fre[bai_temp],bai_temp)
                sum_fre=float(sum(concept_fre))
                for bai_temp_k in range(len(baidutag)):
                    temp_confre[baidutag[bai_temp_k]]=float(concept_fre[bai_temp_k])/sum_fre
                current_pteq[e]=temp_confre
                current_pvep_pre=querySQL.Query(self.sql_current_evp%(e,p))[''][0] #计算同一实体e同一意图p的不同值v的个数
                current_e=0 #当前实体对应的频数 分子
                current_alle=0 #对当前value的不同实体记总数 分母
                current_pe=0 #当前实体，对应类别（p,e）共同满足的个数
                current_allp=0 #当前实体的频数在整个EV中，作为求类别的分母。
                for que_ev in self.EV: #整个for循环就把所有的实体遍历所有问题
                    current_evi=list(que_ev.values())[0] #当前EV当前问题的所有实体对
                    for key1 in current_evi.keys():  #对于每一个实体对
                        e1,p1,v1=key1.split("&&&&&")
                        if v ==v1: #如果value相同
                            current_alle+=1 #对应实体的value其他共有多少实体的频数
                            if e1==e:current_e+=1 #如果并且是与当前问题实体相同，当前实体加一
                        if e1==e:#对于当前类别的个数 分子，由于实体e可能出现多次，对于第一个计算只是重复，并没有影响
                            current_allp+=1
                            if p1.lower()=='baidutag':#并且是当前实体的类别
                                # if v1 not in e_c_pre:e_c_pre[v1]=1
                                # else:e_c_pre[v1]+=1
                                current_pe+=1#并且是当前实体对应的分类标
                # if e not in current_pteq:
                #     for key_b,value in e_c_pre.items():
                #         e_c_pre[key_b]=float(value)/float(current_allp)
                #     current_pteq[e]=e_c_pre
                currente_pre1[e]=float(current_e) #对应第一个分子
                if e in currente_pre2: currente_pre2[e]+=float(current_alle) #把所有实体value对应实体的分母相加
                else: currente_pre2[e]=float(current_alle) #然后把相同实体的分母相加

                current_pvep[key]=1.0/float(current_pvep_pre)#之所以分子为1，原因是，如果在KB中e,p一旦确定，对应的数值给定均匀概率，并且公式只有value在变，
        for key_5,value5 in currente_pre1.items():
            currente_pre1[key_5]=float(value5)/float(currente_pre2[key_5])
        return currente_pre1,current_pteq,current_pvep
    def calculate_ppeq(self):
        """
        此函数是为了求解当前问题每一个实体对应的类别的概率p(c|q,e)=p(c)*p(e|c),首先计算当前问题q的当前可能实体e，
        对应类别c的概率p(c),分子为对应类别的个数，分母为所有EV对中的类别总数；记录到一个字典中，以防止重复计算。
        然后计算p(e|c) 就是在当前类别下，QA共有多少个实体e计数（分子），分母是类别c的个数。这样当前实体e对应类别
        c的概率就求出来了。怎么存储，仍然以当前问题{q,e,p:pre},其中p代表类别c,pre代表对应的概率;暂且把所有的基于QA实体
        对应的类别作为固定概率求解返回形式：
        （e,p,v）:{c1:pre1,c2:pre2,...,pren}
        :param quei:当前问题
        :return: 返回当前问题每个实体对应类别的p的概率
        """
        c_class = {}  # 用来保存所有类别以及对应的频数,最终保存对应类别的频率p(ck)
        c_class_pre={}
        ce_class={}#用来保存对应类别，不同实体的频数p(ei|ck) 分母是ck发生的频数，对于没有在ck的实体统一为p(ck)
        ce_class_pre={}#
        total_class = 0  # QA语料中类别总频数
        all_e=[] #所有的实体
        c_include_e={}

        for que_ev in self.EV:
            # print(que_ev.keys()) #当前问题
            current_evi = list(que_ev.values())[0]  # 当前EV当前问题的所有实体对
            for key1 in current_evi.keys():  # 对于每一个实体对
                e1, p1, v1 = key1.split("&&&&6")
                all_e.append(e1)
                if p1.lower()=='baidutag':
                    total_class+=1
                    if v1 in c_class:c_class[v1]+=1
                    else:c_class[v1] = 1
                    if '^'.join([e1,v1]) in ce_class:ce_class['^'.join([e1,v1])]+=1
                    else:ce_class['^'.join([e1,v1])]=1
        all_e=list(set(all_e))
        cestore=list(c_class.keys())#对应的全部类别
        print(c_class)
        print(ce_class)
        final_peped={}#最终的对于所有实体e的概率下每一个实体的可能概率
        #把每个类别的总频数转换为频率,就把对应的p(ck)求出。  #求解条件转化概率
        for c_class_key1,c_class_value in c_class.items():
            c_class_pre[c_class_key1]=float(c_class_value)/float(total_class)#概率p(ck)
        for c_key in cestore:#c_key对应类别,对应类别的实体e_value
            #下边这个循环如果运行完成，则对应当前类别的所有p(ei/ck)已经计算完成，可以进行p(ei/ck)*p(ck)
            reall_key=[c_key]#包含了对应类别的实际实体
            ce_class_pre=1
            #以下循环把p(ei/ck)全部想乘
            for ce_class_key,ce_class_value in ce_class.items():#key 对应实体e和对应类别c value对应频数
                if ce_class_key.split('^')[1] == c_key: #如果实体有对应的当前类别,用拉普拉斯平滑
                    reall_key.append(ce_class_key.split('^')[0])
                    ce_class_pre*=float(ce_class_value+c_class[c_key])/float(c_class[ce_class_key.split("^")[1]]+total_class)
                else:#如果实体对于当前类别为0，用拉普拉斯平滑
                    ce_class_pre*=float(c_class[c_key])/float(c_class[ce_class_key.split("^")[1]]+total_class)

            final_peped['^'.join(reall_key)]=c_class[c_key]*ce_class_pre

        print(c_class_pre)
        print(final_peped)




if __name__=="__main__":
    EV = open("./../data/pqev_final.pkl", 'rb')
    EV = pickle.load(EV)
    reall_ev=open("./../data/EV_two.pkl",'rb')
    entity_values=pickle.load(reall_ev)
    value_entities=pickle.load(reall_ev)
    pro_onlines=Pro_onlines(EV)
    que1=pro_onlines.EV[6]
    print(que1)
    que_pre,que_ppeq,que_pvep=pro_onlines.calculate_piq_kb(que1)
    print(que_pre)
    print(que_ppeq)
    print(que_pvep)
    # pro_onlines.calculate_ppeq()




