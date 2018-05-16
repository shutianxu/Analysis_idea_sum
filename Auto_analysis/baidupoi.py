import json
import requests
from urllib import request
from urllib.parse import quote
import pandas as pd
from requests.exceptions import RequestException



headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36'}
def get_one_page(url):
    try:
        response = requests.get(url,headers = headers)
        if response.status_code ==200:
            return response.text
        return None
    except RequestException:
        return None
    
    
# =============================================================================
# def parse_one_page(html):
# 
#     
#     
#     return items
# =============================================================================

# 百度地图poi：http://api.map.baidu.com/place/v2/search
# 请替换为自己申请的key值：申请Web服务API类型KEY http://lbsyun.baidu.com/apiconsole/key?application=key
# ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO
# http://api.map.baidu.com/place/v2/search?query=卫生服务中心&tag=医疗&page_size=20&page_num=0&scope=2&region=上海&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO

tag_11 = ['美食' ,'酒店' ,'购物' ,'生活服务' ,'丽人' ,'旅游景点' ,'休闲娱乐' ,'运动健身' ,'教育培训' ,'文化传媒' ,'医疗' ,'汽车服务' ,'交通设施' ,'金融' ,'房地产' ,'公司企业' ,'政府机构' ,'出入口' ,'自然地物']
# =============================================================================
# city =['吉首市','白山市','徐州市','深圳市','鸡西市','佛山市','眉山市','潍坊市','荆门市','秦皇岛市','拉萨市','长沙市','湘潭市','广州市','雅安市','宝鸡市','宣城市','大连市','廊坊市','塔城市','鞍山市','菏泽市','淮北市','九江市','黑河市','巴彦浩特镇','遂宁市','桂林市','长春市','云浮市','昌都市','吕梁市','平凉市','达州市','铁岭市','商洛市','贵港市','乌海市','江门市','昭通市','烟台市','葫芦岛市','德阳市','西宁市','玉溪市','景洪市','克拉玛依市','漳州市','林芝市','晋城市','那曲县','濮阳市','揭阳市','昌吉市','石家庄市','唐山市','抚州市','保山市','漯河市','新余市','三亚市','永州市','平顶山市','青岛市','合作市','崇左市','玉树市','大庆市','蚌埠市','株洲市','扬州市','南宁市','南京市','郴州市','衡水市','珠海市','邯郸市','蒙自市','信阳市','北海市','台州市','肇庆市','佳木斯市','咸宁市','武威市','榆林市','运城市','舟山市','临沂市','湛江市','衡阳市','共和县','孝感市','张掖市','巴中市','河池市','玛沁县','山南市','莆田市','驻马店市','梅州市','鹰潭市','海东市','陇南市','延吉市','乐山市','遵义市','韶关市','汕尾市','阿勒泰市','锡林浩特市','绥化市','邢台市','张家界市德州市','西安市','嘉兴市','娄底市','银川市','黄山市','岳阳市','固原市','湖州市','白银市','丹东市','中山市','百色市','宿州市','恩施市','荆州市','周口市','咸阳市','邵阳市','阿图什市','盘锦市','内江市','攀枝花市','景德镇市','池州市','承德市','辽源市','六安市','大同市','宁波市','萍乡市','安庆市','十堰市','郑州市','香格里拉市','广安市','商丘市','厦门市','开封市','本溪市','晋中市','宿迁市','昆明市','库尔勒市','锦州市','贺州市','合肥市','绍兴市','四平市','楚雄市','朔州市','哈密市','噶尔县','南阳市','延安市','西昌市','茂名市','清远市','随州市','加格达奇','阜新市','龙岩市','泰州市','嘉峪关市','泸州市','丽江市','宜春市','康定县','常德市','防城港市','铜仁市','阿克苏市','南通市','阳江市','渭南市','沧州市','宜昌市','儋州市','来宾市','芜湖市','辽阳市','枣庄市','铜川市','呼和浩特市','衢州市','六盘水市','广元市','阳泉市','三沙市','曲靖市','包头市','和田市','黄石市','安康市','吉安市','沈阳市','泸水县','马鞍山市','天水市','赣州市','淮南市','玉林市','定西市','福州市','怀化市','滨州市','成都市','齐齐哈尔市','呼伦贝尔市','潮州市','威海市','伊宁市','贵阳市','临夏市','河源市','鹤壁市','牡丹江市','东营市','无锡市','襄阳市','丽水市','阜阳市','新乡市','七台河市','普洱市','石嘴山市','忻州市','金昌市','连云港市','鄂州市','大理市','汉中市','金华市','海晏县','张家口市','中卫市','绵阳市','柳州市','海口市','营口市','黄冈市','上饶市','博乐市','松原市','双鸭山市','温州市','喀什市','日照市','滁州市','南充市','毕节市','淄博市','兴义市','鹤岗市','常州市','德令哈市','同仁县','吐鲁番市','日喀则市','南昌市','洛阳市','安阳市','朝阳市','自贡市','乌兰察布市','资阳市','盐城市','梧州市','武汉市','苏州市','抚顺市','白城市','兰州市','太原市','哈尔滨市','巴彦淖尔市','安顺市','镇江市','益阳市','都匀市','济南市','赤峰市','通辽市','泉州市','文山市','吉林市','庆阳市','吴忠市','钦州市','通化市','莱芜市','三明市','东莞市','宜宾市','伊春市','马尔康县','淮安市','酒泉市','许昌市','泰安市','杭州市','焦作市','南平市','长治市','临沧市','保定市','济宁市','乌鲁木齐市','汕头市','铜陵市','临汾市','惠州市','亳州市','乌兰浩特市','聊城市','三门峡市','芒市','凯里市','鄂尔多斯市','宁德市','北京市','上海市','重庆市','天津市']
# =============================================================================
city_11 = ['上海市']
query_meishi = ['中餐厅','外国餐厅','小吃快餐店','蛋糕甜品店','咖啡厅','茶座','酒吧']
query_jiudian = ['星级酒店','快捷酒店','公寓式酒店']
query_gouwu = ['购物中心','百货商场','超市','便利店','家居建材','家电数码','商铺','集市']
query_shenghuo = ['通讯营业厅','邮局','物流公司','售票处','洗衣店','图文快印店','照相馆','房产中介机构','公用事业','维修点','家政服务','殡葬服务','彩票销售点','宠物服务','报刊亭','公共厕所']
query_liren = ['美容','美发','美甲','美体']
query_lvyou = ['公园','动物园','植物园','游乐园','博物馆','水族馆','海滨浴场','文物古迹','教堂','风景区']
query_xiuxian = ['度假村','农家院','电影院','KTV','剧院','歌舞厅','网吧','游戏场所','洗浴按摩','休闲广场']
query_yundong = ['体育场馆','极限运动场所','健身中心']
query_jiaoyu = ['高等院校','中学','小学','幼儿园','成人教育','亲子教育','特殊教育学校','留学中介机构','科研机构','培训机构','图书馆','科技馆']
query_wenhua = ['新闻出版','广播电视','艺术团体','美术馆','展览馆','文化宫']
query_yiliao = ['综合医院','专科医院','诊所','药店','体检机构','疗养院','急救中心','疾控中心']
query_qiche = ['汽车销售','汽车维修','汽车美容','汽车配件','汽车租赁','汽车检测场']
query_jiaotong = ['飞机场','火车站','地铁站','长途汽车站','公交车站','港口','停车场','加油加气站','服务区','收费站','桥','充电站','路测停车位']
query_jinrong = ['银行','ATM','信用社','投资理财','典当行']
query_fangdichan = ['写字楼','住宅区','宿舍']
query_gongsi = ['公司','园区','农林园艺','厂矿']
query_zhengfu = ['中央机构','各级政府','行政单位','公检法机构','涉外机构','党派团体','福利机构','政治教育机构']
query_churukou = ['高速公路出口','高速公路入口','机场出口','机场入口','车站出口','车站入口','门','停车场出入口']
query_zirandiwu = ['岛屿','山峰','水系']

name = []
lat = []
lng =[]
address =[]
prov =[]
city =[]
area =[]
tag =[]
detail_info_type =[]
num = 0
for k in city_11:
    for i in tag_11:   
        if i == '美食':
            for j in query_meishi:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html)                         
                        df = pd.DataFrame()
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])
                        
                        
                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False) 
                                                
                        print(df)
                        print(num)
                        num = num + 1
                    except:
                        print(':(')


        elif i == '酒店':
            for j in query_jiudian:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        
                        df = pd.DataFrame()
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        
                                        
                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(df)
                        print(num)
                        num = num + 1

                    except:
                        print(':(')                        
                    
                    
                    
        elif i == '购物':
            for j in query_gouwu:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])
                        
                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(df)
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '生活服务':
            for j in query_shenghuo:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])
                            

                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                                                 
                        
                        print(df)
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '丽人':
            for j in query_liren:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        
                      
                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '旅游景点':
            for j in query_lvyou:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        
                       
                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '休闲娱乐':
            for j in query_xiuxian:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        

                        
                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '运动健身':
            for j in query_yundong:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        

                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                                                 
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '教育培训':
            for j in query_jiaoyu:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        
                        

                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '文化传媒':
            for j in query_wenhua:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        
                        

                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '医疗':
            for j in query_yiliao:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        
                        
                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '汽车服务': 
            for j in query_qiche:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        

                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '交通设施':
            for j in query_jiaotong:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        

                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '金融':
            for j in query_jinrong:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        
                        
                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '房地产':
            for j in query_fangdichan:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        

                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '公司企业':
            for j in query_gongsi:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])                        

                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '政府机构':
            for j in query_zhengfu:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])
                        
                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '出入口':
            for j in query_churukou:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])
                        
                        
                        
                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')

        elif i == '自然地物':
            for j in query_zirandiwu:
                for h in range(20):
                    try:
                        url = 'http://api.map.baidu.com/place/v2/search?query='+str(j)+'&tag='+str(i)+'&page_size=20&page_num='+str(h)+'&scope=2&region='+str(k)+'&coord_type=3&output=json&ak=8VUjfqGwgnHwEZLxwPpnZvO1Sgeq2HFO'
                        html = get_one_page(url)                        
                        data = json.loads(html) 
                        df = pd.DataFrame()
                        
                        for ll in range(20):
                            name.append(data["results"][ll]["name"])
                            lat.append(data["results"][ll]["location"]["lat"])
                            lng.append(data["results"][ll]["location"]["lng"])
                            address.append(data["results"][ll]["address"])
                            prov.append(data["results"][ll]["province"])
                            city.append(data["results"][ll]["city"])
                            area.append(data["results"][ll]["area"])
                            tag.append(data["results"][ll]["detail_info"]["tag"])
                            detail_info_type.append(data["results"][ll]["detail_info"]["type"])
                        
                        
                        
                        df = pd.DataFrame({ 
                                'name' : name,                  
                                'lat' :  lat,
                                'lng' : lng,
                                'address' :  address,
                                'prov' : prov,
                                'city' : city,
                                'area' : area,
                                'tag' : tag,
                                'detail_info_type' : detail_info_type
                                })
                            
                        df.to_csv('D:/document/crawling/baidu_poi.csv',index = False)                         
                                                
                        print(data["results"][0]["name"])
                        print(data["results"][0]["location"]["lat"])
                        print(data["results"][0]["location"]["lng"])                   
                        print(data["results"][0]["address"])                       
                        print(data["results"][0]["province"])
                        print(data["results"][0]["city"])
                        print(data["results"][0]["area"])
                        print(data["results"][0]["detail_info"]["tag"])
                        print(data["results"][0]["detail_info"]["type"])
                        print(num)
                        num = num + 1
                    except:
                        print(':(')
