#! -*- coding:utf-8 -*-
"""
利用em算法计算p(p/t)
"""
import pickle
import matplotlib.pyplot as plt
import time
import re
import copy
from caculate_pro import *
from caculate_pro import Pro_onlines
from connectSQLServer import connectSQL
host = '172.16.54.33'
user = 'sa'
password = 'chentian184616_'
database= 'chentian'
querySQL = connectSQL(host, user, password, database)
class Em(object):
    def __init__(self):
        self.EV=open("./../data/pqev_final.pkl",'rb')
        self.EV=pickle.load(self.EV)
        self.stopwords = [words.strip('\n') for words in open('./../data/stopwords1.txt', 'r', encoding='gbk')]
        self.calculate_proba=Pro_onlines(self.EV)
        pkl_file = open('./../data/entity_template.pkl', 'rb')
        self.template_property=pickle.load(pkl_file)
        self.entity_template = pickle.load(pkl_file)
        self.template_entity = pickle.load(pkl_file)  # 模板以及对应的实体
        self.S=0#迭代次数初始化
        self.sql="SELECT * FROM [chentian].[dbo].[baike_triples1] WHERE entity ='%s' AND property='BaiduTAG'"
        self.sql1="SELECT * FROM [chentian].[dbo].[baike_triples1] WHERE entity in %(name)s "
        self.ppt_param = pickle.load(open('./../data/ppt_simple.pkl', 'rb'))  # key 为对应的模板，value对应Property的概率
        # self.ppt_param_old=self.ppt_param
        self.ppt_param_final = {}
        # self.entity_template = pickle.load(open('./../data/entity_template.pkl', 'rb'))

    def init_parameter1(self):
        """
        对参数进行初始化，详细解析见文档：基于知识图谱的智能问答框架.docx
        :return: 返回初始化参数
        """
        i = 0 #运行了多少个问题，一个问题有多个模板
        print(len(self.EV))
        entity_template = {}  # 把对应当前问题的所有可能模板全部存储,key位e，value为template
        template_entity = {}  # 把对应当前问题的所有可能模板全部存储,key位template，value为e
        template_property={}  #把当前所有模板与property存储
        for que_ev in self.EV:  # 整个for循环就把所有的实体遍历所有问题
            question=list(que_ev.keys())[0]
            current_evi = list(que_ev.values())[0]  # 当前EV当前问题的所有实体对
            for key1 in current_evi.keys():  # 对于当前问题的每一个实体对
                e1, p1, v1 = key1.split("&&&&&")
                if e1!='' and p1!='' and v1!='':
                    # if question=='擎天柱真车什么车':
                    temp_sql = self.sql % e1#一次性就把当前实体所有的baidutag拿出来了
                    result = list(querySQL.Query(temp_sql)['value'])#这是实际的所有概念,也就是类别
                    for concept in result:
                        if question[-1] in self.stopwords:question=question[:-1]
                        temp_template=question.replace(e1.split("（")[0],'$$$$$'+concept+"$$$$$")#对应concept形成问题模板
                        if temp_template in template_property:template_property[temp_template].append(p1)
                        else:template_property[temp_template] = [p1]
                        if temp_template in template_entity:template_entity[temp_template].append(e1)
                        else:template_entity[temp_template]=[e1]
                        if e1 in entity_template:entity_template[e1].append(temp_template)
                        else:entity_template[e1]=[temp_template]
                # #剩余实体e对应的概念添加对应实体el对应的模板到entity_template
                # for e,category in entity_catory.items():
                #     if len(category)==0:continue
                #     tem_template=question.replace(e.split("（")[0], '$$' + v1 + "$$")
                #     if e in entity_template:entity_template[e].append(tem_template)
                #     else:entity_template[e] = [tem_template]
                #     if tem_template in template_entity:template_entity[tem_template].append(e)
                #     else:template_entity[tem_template] = [e]
            if i%100==0:
                print(i,"运行了i个问题")
            i+=1
        output = open('./../data/entity_template.pkl', 'wb')
        pickle.dump(template_property,output)
        pickle.dump(entity_template, output)
        pickle.dump(template_entity,output)
        output.close()
    def init_paramter2(self):
        #对于template_entity为key不同的模板，value为不同的实体{template:e1,e2,e3,...}
        #对应模板的每一个实体有多个属性，并且，求出模板多少个属性，
        #返回结果为{template1：{property1:0.1,property2:0.3},template2:{property1:0.02}}
        #就为最终的初始化参数结果,实体以及属性必须满足p(v|e,p)>0,也就是p必须与答案相关。
        pkl_file=open('./../data/entity_template.pkl','rb')
        self.template_property=pickle.load(pkl_file)
        self.entity_template=pickle.load(pkl_file)
        self.template_entity=pickle.load(pkl_file) #模板以及对应的实体
        ppt={} #{template1：{property1:0.1,property2:0.3},template2:{property1:0.02}}  就是模型参数，property必须与答案相关
        i=0
        for template,entity_set in self.template_property.items():
            entity_set=list(set(entity_set))
            # print(template)
            # print(entity_set)
            # print("%%%%%%")
            pro=[]
            pre={}
            # real_entity = [k.replace("'", "''") for k in entity_set]  # 由于实体中可能包含',则替换为'' 在数据库中就认为是单引号
            # if len(real_entity) == 1:
            #     real_entity = "('" + str(real_entity[0]) + "')"
            # else:
            #     real_entity = tuple(real_entity)
            # temp_sql = self.sql1 % {'name':real_entity}   # 一次性就把所有的baidutag拿出来了
            # result = list(querySQL.Query(temp_sql)['property'])  # 这是实际的所有对应实体的属性
            # pro.extend(result)
            # pro=list(set(pro))
            pro_len=len(entity_set)
            for p in entity_set:
                pre[p]=1.0/float(pro_len)
            ppt[template]=pre
            if i%100==0:
                print(i)
            i+=1
        output = open('./../data/ppt_simple.pkl', 'wb')
        pickle.dump(ppt, output)
        output.close()
    def three_fre(self):
        three_fres=[]
        i=0
        for que_ev in self.EV:  # 对应论文中的 i=1,....,m对于当前问题
            que_pre = self.calculate_proba.calculate_piq_kb(que_ev)  # 当前问题的三个概率的所有值,可以直接计算保存
            three_fres.append(que_pre)
            if i%100==0:print(i,"处理数据集")
            i+=1
        output = open('./../data/three_fres.pkl', 'wb')
        pickle.dump(three_fres, output)
        output.close()

    def E_STEP(self):
        """
        f(x = (q; e; v); z = (p; t)) = P(q)P(ej|q)P(tj|ej; q)P(vj}ej; p)
        利用公式p(zi|X,params)=f(xi,zi)*params求期望
        :return:返回 p(zi|X,params),主要是得到self.ppt_param_old 以及更新 self.ppt_param ，
        """
        self.three_fre=pickle.load(open('./../data/three_fres.pkl', 'rb'))
        i=0
        self.ppt_param_old = copy.deepcopy(self.ppt_param)
        template_property_pre_result=[] #获得了训练数据集的所有更新参数的数值,梯度数值
        for que_ev in self.EV:  # 对应论文中的 i=1,....,m对于当前问题
            template_property_pre={} #对应当前问题所有模板的property条件概率值#对应当前模板的每个属行e步的条件概率值形式如下{template1:{pre1:0.2,pre2:0.4},template2:{pre1:3,pre2:6}}
            que_pre, que_ppeq, que_pvep=self.three_fre[i]#当前问题的三个概率的所有值,可以直接计算保存
            evi = list(que_ev.values())[0]  # 当前问题中的所有（实体-属性-值）
            for key in evi.keys():
                e, p, v = key.split("&&&&&")  # 接下来对每一个v 遍历每一个问题中所有的相同v,得到对应的实体e，并且记录实体出现的频数 实体e可能出现多次,对第一个概率没有影响，但是对第二个有影响，本来有结果，
                                              # 重复第二次没有对应baidutag，则会重新赋值为空，
                if e in self.entity_template:templates=self.entity_template[e] #对应实体的所有模板
                else:continue
                if e in que_ppeq:baidutags=que_ppeq[e] #对应实体多个concept的概率
                else:continue
                for template4 in templates:
                    try:
                        property_pre={}
                        baidutag=re.search('\\$.*\\$',template4).group().replace("$","")#这一步其实可以和原问题进行比较，去除相同的
                        temp_ppt_param = self.ppt_param[template4]#对应当前模板中所有属性的概率
                        if len(temp_ppt_param)>1:
                            for key,value in que_pvep.items():
                                if e in key: #如果找到对应实体的属性p
                                    property=key.split("&&&&&")[1]
                                    if property=='BaiduCARD':property_pre[property]=0
                                    else:
                                        # 前三个是f(xi,zi=(p,t))*param(p,t)的概率计算完成，后一个是参数的数值表示（t,p）的初始或者训练概率在ppt.pkl文件中
                                        try:#之所以try是由于有的proerpty在模板中概率为0
                                            pe1q = que_pre[e] * baidutags[baidutag]*value*temp_ppt_param[property]
                                            property_pre[property]=pe1q
                                        except Exception as e1:
                                            continue
                        else:
                            property_pre[list(temp_ppt_param.keys())[0]]=0 #如果只有一个predict，那么就不用更新
                        if template4 in template_property_pre:
                            for key_prop,value_prop in property_pre.items():
                                if key_prop in template_property_pre[template4]:
                                    template_property_pre[template4][key_prop]+=property_pre[key_prop]
                                else:
                                    template_property_pre[template4][key_prop]=property_pre[key_prop]
                        else:template_property_pre[template4]=property_pre
                    except Exception as e2:continue
            template_property_pre_result.append(template_property_pre)
            # if i%1000==0:
            #     print(i,len(self.EV))
            i += 1
        #接下来是M_step，共需要两个数据一个是参数，一个是template_property_pre_result
        for template_property_pre1 in template_property_pre_result:#其中template_property_pre1多个模板{template1:{pre1:0.2,pre2:0.4},template2:{pre1:3,pre2:6}}
            for key_now,value_now in template_property_pre1.items():#key_now表示对应模板，value_now表示对应的{property:0.35}
                if len(value_now)==0:
                    continue
                else:#如果模板属性概率不为0，对参数进行更新。
                    for property_key,property_value in self.ppt_param[key_now].items():  # 对应当前模板中所有属性的概率
                         if len(self.ppt_param_final)!=0 and property_key in self.ppt_param_final[key_now]:continue#如果参数已经更新完毕就不再更新
                         else:
                             if property_key in value_now:
                                self.ppt_param[key_now][property_key]+=value_now[property_key] #对参数进行更新
                             else:continue
        #对self.ppt_param在进行最后的归一化处理
        for template,property_dict in self.ppt_param.items():
            property_sum=sum(property_dict.values())
            for property_key,property_value in property_dict.items():
                self.ppt_param[template][property_key]=property_value/property_sum
    def evaluate(self):
        """
        这个函数是main函数，对此进行训练，得到最终参数
        """
        ax1 = plt.subplot(211, title='recall_rate')
        current_step=0
        self.E_STEP()#这一步是第一次更新，也是为了得到ppt_param_old
        start = time.time()
        flag=True
        while flag:
            flag, dist = self.convergence_param(self.ppt_param_old, self.ppt_param, current_step=current_step)
            if flag==False:break
            self.E_STEP()
            plt.bar(current_step, abs(dist))
            plt.sca(ax1)
            plt.pause(0.2)
            current_step+=1
            end = time.time()
            print("消耗时间：",str(dist)+str(end - start), "秒")
            # print(self.convergence_param(self.ppt_param_old,self.ppt_param,current_step=current_step))
        plt.savefig('./../data/train_update1.jpg')
        output = open('./../data/ppt_update_update1.pkl', 'wb')
        pickle.dump(self.ppt_param_old,output)
        pickle.dump(self.ppt_param, output)
        output.close()
    def convergence_param(self,old_template,new_template,thread=0.00001,step=200,current_step=0):
        """
        old_template：s步参数，new_template为s+1步参数
        此函数判断参数是否收敛，也就是训练概率p(p|t)
        :param thread: 对应收敛的参数更新前后差值，step为迭代次数，current_step 当前迭代次数
        :return: True 或者False，一旦存储到 self.ppt_param_final 中的参数，就不再更新
        """
        i=0
        distance=0
        for template_key,template_value in new_template.items():
            for property_key,property_value in template_value.items():
                distance+=abs(property_value-old_template[template_key][property_key])
                if property_value-old_template[template_key][property_key]<thread:#证明满足罚值约束，不再更新，条件到ppt_param_final
                    if len(self.ppt_param_final)!=0 and template_key in self.ppt_param_final:
                        if property_key in self.ppt_param_final[template_key]:#如果对应的属性property在最后存储，说明已经不需要更新
                            continue
                        else:
                            self.ppt_param_final[template_key][property_key] = property_value
                    else:
                        self.ppt_param_final[template_key]={}
                        self.ppt_param_final[template_key][property_key]=property_value
                else:
                    i+=1
        if i==0 or current_step>=step:#如果i为0，表示没有更新的参数了，current_step当前迭代步数
            print(i)
            print(current_step,"当前迭代步数False")
            return False,distance
        else:
            print(current_step,"当前迭代步数True")
            return True,distance
if __name__=="__main__":
    pro_onlines=Em()
    print(len(pro_onlines.EV))
    pro_onlines.three_fre()
    start_time=time.time()
    pro_onlines.E_STEP()
    end_time=time.time()
    print("消耗的时间为" + str(end_time - start_time) + "秒")
    print(pro_onlines.ppt_param)
    pro_onlines.evaluate() #总函数
    # pro_onlines.init_parameter1()
    # ppt0=pro_onlines.init_paramter2()
    # for template,value in ppt0.items():
    #     print(template)
    #     print(value)
    #     print("&&&&&&&&&&&&")
    old_ppt_param = pickle.load(open('./../data/ppt_update_update1.pkl', 'rb'))
    # file=open('./../data/ppt_simple.pkl', 'rb')
    # old_ppt_param= pickle.load(file)
    # new_ppt_param=pickle.load(file)
    # for ele in old_ppt_param:
    #     print(ele)

    for ele,value in old_ppt_param.items():
        print(ele)
        print(value)
    #     # print(new_ppt_param[ele])
    #     print("……………………………………………………………………")
    #     print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
        # print(new_key)
        # print(new_value)
    # for tempalte,value in old_ppt_param.items():
    #     print(tempalte)
    #     print(value)
    # new_ppt_param =pickle.load(open('./../data/ppt_update.pkl', 'rb'))
    # bools=pro_onlines.convergence_param(old_template=old_ppt_param,new_template=new_ppt_param)
    # print(bools)