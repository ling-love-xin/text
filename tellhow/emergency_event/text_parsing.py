# -*- coding: utf-8 -*-
# @Time : 2019/11/11 9:10
# @Author : len
# @Email : ysling129@126.com
# @File : text_parsing
# @Project : tellhow
# @description :
import re
import pandas as pd
import emergency_event
from conf import rules

def get_people_hurt_num(
        uppdate_df: pd.DataFrame,
        illness_list: list,
        illegal_list: list,
        pattern_first_situation_str: str,
        pattern_second_situation_str: str,
        pattern_brackets_Chinese_str: str,
        pattern_brackets_English_str: str,
        pattern_discribe_str: str,
        pattern_Arabic_numerals_str: str,
        pattern_Chinese_numerals_str: str,
        pattern_time_str: str,
        pattern_qizhong_str: str,
        label_list: list):
    # 第一种情况，事故描述中有 造成... 这样格式的描述，在这样描述中通常描述了事故伤亡的人数
    # 针对这种情况，检测 “造成”到句子结尾之间 有几个伤亡人数
    pattern_first_situation = re.compile(pattern_first_situation_str)

    # 第二种情况，事故描述为 导致/致....死亡/受伤
    pattern_second_situation = re.compile(pattern_second_situation_str)

    # 第三种情况，事故描述为 人名（），人名（），...死亡。 ... 受伤 这种格式的，可以通过前文中括号的数量判断,
    # 匹配空格中的内容，如果空格中的内容在非描述任务内容列表中，则不将该括号的数量计算进入
    # 匹配中文格式的括号
    pattern_brackets_Chinese = re.compile(pattern_brackets_Chinese_str)
    # 匹配英文格式的括号
    pattern_brackets_English = re.compile(pattern_brackets_English_str)

    # 用来匹配在第一种情况和第二种情况中的 造成。。。/ 导致。。。 描述中的括号内部的内容，在解析过程中需要将括号中的内容先去掉，避免造成干扰
    pattern_discribe = re.compile(pattern_discribe_str)

    # 匹配阿拉伯数字
    pattern_Arabic_numerals = re.compile(pattern_Arabic_numerals_str)
    # 匹配中文数字
    pattern_Chinese_numerals = re.compile(pattern_Chinese_numerals_str)

    # 匹配描述事件发生时间的数字格式，这些数字会对解析过程产生干扰，需要去除
    pattern_time = re.compile(pattern_time_str)

    # 匹配其中。。。。，在其中里边的描述可能会包含有 无明显外伤之类的描述，需要将无明显外伤这类的人数去除
    pattern_qizhong = re.compile(pattern_qizhong_str)

    # 设置解析过程中用到的变量
    # 设置中文数字到阿拉伯数字的转换字典
    convert_dict = {'一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6,
                    '七': 7, '八': 8, '九': 9, '十': 10}
    resultlist = []

    # 开始解析
    for i, row in enumerate(uppdate_df.iterrows()):
        row = row[1]
        item = row['evt_memo']
    # for item in df_people_hurt:
        count_death = 0
        count_injury = 0
        count_injury_h = 0
        # 对应上述的第一种情况
        if pattern_first_situation.findall(item):
            temp = re.sub(
                pattern_discribe,
                '',
                pattern_first_situation.findall(item)[0],
                count=0).split('，')
            if '' in temp:
                temp.remove('')
            for sen in temp:
                if sen[0:2] == '其中' and '无明显外伤' not in sen:
                    break
                # 如果分句结果中有“死亡”，该分句是描述死亡情况的
                if '死亡' in sen or '身亡' in sen:
                    # 如果在分句中不包含 、 及 阿拉伯数字 中文数字 说明只有一个人死亡了，死亡人数加1
                    if "、" not in sen and "及" not in sen and "和" not in sen and not pattern_Arabic_numerals.findall(
                            sen) and not pattern_Chinese_numerals.findall(sen):
                        count_death += 1
                    # 如果在分句中包含 、 或者 及 不包含 阿拉伯数字
                    # 中文数字，说明有多人死亡，利用find_str_num()函数进行搜索
                    elif ("、" in sen or "及" in sen or "和" in sen) and (not pattern_Arabic_numerals.findall(sen) and not
                                                                       pattern_Chinese_numerals.findall(sen)):
                        count_death += find_all_num(sen, label_list)
                    # 如果在分句中不包含 、 及 包含 阿拉伯数字,说明分句中有用阿拉伯数字明确描述的死亡人数，
                    elif pattern_Arabic_numerals.findall(sen):
                        # 原始
                        # count_death += int(pattern_Arabic_numerals.findall(sen)[0])
                        # 新修改，需要检验修改是否有问题
                        if "、" in sen or "及" in sen or "和" in sen:
                            count_death += \
                                find_all_num_update(sen, label_list, pattern_Arabic_numerals, pattern_Chinese_numerals,
                                                    illness_list)[0]
                            count_injury += \
                                find_all_num_update(sen, label_list, pattern_Arabic_numerals, pattern_Chinese_numerals,
                                                    illness_list)[1]
                        else:
                            count_death += int(
                                pattern_Arabic_numerals.findall(sen)[0])
                    elif pattern_Chinese_numerals.findall(sen):
                        index = sen.find('死亡')
                        num_ch = pattern_Chinese_numerals.findall(sen[0:index])[
                            0][0]
                        count_death += convert_dict[num_ch]
                        # 如果死亡之后还有数字，数字应该是描写受伤人数的，如：造成一人死亡两人皮外伤。
                        if pattern_Chinese_numerals.findall(sen[index + 1:]):
                            num_ch_last = pattern_Chinese_numerals.findall(
                                sen[index + 1:])[0][0]
                            count_injury += convert_dict[num_ch_last]
                # 如果在分句中不包含死亡，那大概率是描述受伤的人数
                elif '死亡' not in sen and (('伤' in sen and '伤者' not in sen) or '被埋' in sen or '症状' in sen or
                                          '骨折' in sen or '摔倒' in sen or '自行逃出' in sen):
                    # 如果在分句中不包含 、 及 阿拉伯数字 中文数字 说明只有一个人受伤了，受伤人数加1
                    if "、" not in sen and "及" not in sen and not pattern_Arabic_numerals.findall(
                            sen) and not pattern_Chinese_numerals.findall(sen):
                        count_injury += 1
                    # 如果在分句中包含 、 或者 及 不包含 阿拉伯数字
                    # 中文数字，说明有多人受伤，利用find_all_num()函数进行搜索
                    elif ("、" in sen or "及" in sen or "和" in sen) and (not pattern_Arabic_numerals.findall(sen) and not
                                                                       pattern_Chinese_numerals.findall(sen)):
                        count_injury += find_all_num(sen, label_list)
                    # 如果在分句中包含 阿拉伯数字,说明分句中有用阿拉伯数字明确描述的受伤人数
                    elif pattern_Arabic_numerals.findall(sen):
                        if "、" in sen or "及" in sen or "和" in sen:
                            count_injury += \
                                find_all_num_update(sen, label_list, pattern_Arabic_numerals, pattern_Chinese_numerals,
                                                    illness_list)[0] + \
                                find_all_num_update(sen, label_list, pattern_Arabic_numerals, pattern_Chinese_numerals,
                                                    illness_list)[1]
                        else:
                            count_injury += int(
                                pattern_Arabic_numerals.findall(sen)[0])
                    elif pattern_Chinese_numerals.findall(sen):
                        index = sen.find('受伤')
                        num_ch = pattern_Chinese_numerals.findall(sen[0:index])[
                            0][0]
                        count_injury += convert_dict[num_ch]
                        # 如果死亡之后还有数字，数字应该是描写受伤人数的，如：造成一人死亡两人皮外伤。
                        if pattern_Chinese_numerals.findall(sen[index + 1:]):
                            num_ch_last = pattern_Chinese_numerals.findall(
                                sen[index + 1:])[0][0]
                            count_injury += convert_dict[num_ch_last]
            # 在分析之后统一对其中。。。的描述进行分析，如果其中有未受伤、无明显外伤之类的关键字
            # 对其中的内容进行解析，如果其中。。。。中描述了 无明显外伤 的人数，需要将这部分人数从总的受伤人数中去除
            injury_num_modify = 0
            death_num_modify = 0
            if pattern_qizhong.findall(item):
                for sen_qizhong in pattern_qizhong.findall(item)[0].split('，'):
                    if '无明显外伤' in sen_qizhong:
                        injury_num_modify += int(
                            pattern_Arabic_numerals.findall(sen_qizhong)[0])
                    # 如果未受伤在其中。。。的描述里边，同时没有数字或者连接词，说明只有一个人，目前只遇到没有数字的情况
                    # 其他情况后续遇到了再进行添加
                    if '未受伤' in sen_qizhong and not pattern_Arabic_numerals.findall(
                            sen_qizhong):
                        injury_num_modify += 1
                    # 如果其中。。。中描述了经。。。抢救无效死亡
                    if '于12日23时23分经抢救无效死亡' in sen_qizhong:
                        death_num_modify += 1

            # 如果其中。。。 中描述了重伤的人数，需要将重伤人数单独整理出来，总的受伤人数要减一
            if pattern_qizhong.findall(item):
                for sen_qizhong in pattern_qizhong.findall(item)[0].split('，'):
                    if '病情危重' in sen_qizhong:
                        if pattern_Arabic_numerals.findall(sen_qizhong):
                            count_injury_h += int(
                                pattern_Arabic_numerals.findall(sen_qizhong)[0])
                        else:
                            count_injury_h += 1
            count_injury -= count_injury_h
            count_injury -= injury_num_modify

            # 如果死亡人数米钱统计为0，同时在描述中有关于死亡关键字
            if count_death == 0 and '死亡' in item:
                count_death += 1
            count_injury -= death_num_modify
            count_death += death_num_modify

        # 对应上述的第二种情况
        elif pattern_second_situation.findall(item):
            temp = re.sub(
                pattern_discribe,
                '',
                pattern_second_situation.findall(item)[0],
                count=0).split('，')
            if '' in temp:
                temp.remove('')
            for sen in temp:
                if sen[0: 2] == '其中' and '较重' in sen:
                    if pattern_Arabic_numerals.findall(sen):
                        count_injury_h += int(
                            pattern_Arabic_numerals.findall(sen)[0])
                    else:
                        count_injury_h += 1
                if sen[0:2] == '其中' or sen[0:2] == '均为':
                    break
                # 如果分句结果中有“死亡”，该分句是描述死亡情况的
                if '死亡' in sen or '身亡' in sen:
                    # 如果在分句中不包含 、 及 阿拉伯数字 中文数字 说明只有一个人死亡了，死亡人数加1
                    if "、" not in sen and "及" not in sen and not pattern_Arabic_numerals.findall(
                            sen) and not pattern_Chinese_numerals.findall(sen):
                        count_death += 1
                    # 如果在分句中包含 、 或者 及 不包含 阿拉伯数字
                    # 中文数字，说明有多人死亡，利用find_str_num()函数进行搜索
                    elif ("、" in sen or "及" in sen) and (not pattern_Arabic_numerals.findall(sen) and
                                                         not pattern_Chinese_numerals.findall(sen)):
                        count_death += find_all_num(sen, label_list)
                    # 如果在分句中不包含 、 及 包含 阿拉伯数字,说明分句中有用阿拉伯数字明确描述的死亡人数，
                    elif pattern_Arabic_numerals.findall(sen):
                        if "、" in sen or "及" in sen or "和" in sen:
                            count_death += \
                                find_all_num_update(sen, label_list, pattern_Arabic_numerals, pattern_Chinese_numerals,
                                                    illness_list)[0]
                            count_injury += \
                                find_all_num_update(sen, label_list, pattern_Arabic_numerals, pattern_Chinese_numerals,
                                                    illness_list)[1]
                        else:
                            count_death += int(
                                pattern_Arabic_numerals.findall(sen)[0])
                    elif pattern_Chinese_numerals.findall(sen):
                        index = sen.find('死亡')
                        num_ch = pattern_Chinese_numerals.findall(sen[0:index])[
                            0][0]
                        count_death += convert_dict[num_ch]
                        # 如果死亡之后还有数字，数字应该是描写受伤人数的，如：造成一人死亡两人皮外伤。
                        if pattern_Chinese_numerals.findall(sen[index + 1:]):
                            num_ch_last = pattern_Chinese_numerals.findall(
                                sen[index + 1:])[0][0]
                            count_injury += convert_dict[num_ch_last]
                # 如果在分句中不包含死亡，那大概率是描述受伤的人数
                elif '死亡' not in sen and ('伤' in sen and '伤者' not in sen) or '被埋' in sen or '骨折' in sen:
                    # 如果在分句中不包含 、 及 阿拉伯数字 中文数字 说明只有一个人死亡了，死亡人数加1
                    if "、" not in sen and "及" not in sen and not pattern_Arabic_numerals.findall(
                            sen) and not pattern_Chinese_numerals.findall(sen):
                        count_injury += 1
                    # 如果在分句中包含 、 或者 及 不包含 阿拉伯数字
                    # 中文数字，说明有多人死亡，利用find_all_num()函数进行搜索
                    elif ("、" in sen or "及" in sen or "和" in sen) and (not pattern_Arabic_numerals.findall(sen) and
                                                                       not pattern_Chinese_numerals.findall(sen)):
                        count_injury += find_all_num(sen, label_list)
                    # 如果在分句中不包含 、 及 包含 阿拉伯数字,说明分句中有用阿拉伯数字明确描述的死亡人数，
                    elif pattern_Arabic_numerals.findall(sen):
                        if "、" in sen or "及" in sen or "和" in sen:
                            count_injury += \
                                find_all_num_update(sen, label_list, pattern_Arabic_numerals, pattern_Chinese_numerals,
                                                    illness_list)[0]
                        else:
                            if sen[sen.find(pattern_Arabic_numerals.findall(sen)[0]) + len(
                                    pattern_Arabic_numerals.findall(sen)[0])] == '路':
                                count_injury += 0
                            else:
                                count_injury += int(
                                    pattern_Arabic_numerals.findall(sen)[0])
                    elif pattern_Chinese_numerals.findall(sen):
                        index = sen.find('受伤')
                        num_ch = pattern_Chinese_numerals.findall(sen[0:index])[
                            0][0]
                        count_injury += convert_dict[num_ch]
                        # 如果死亡之后还有数字，数字应该是描写受伤人数的，如：造成一人死亡两人皮外伤。
                        if pattern_Chinese_numerals.findall(sen[index + 1:]):
                            num_ch_last = pattern_Chinese_numerals.findall(
                                sen[index + 1:])[0][0]
                            count_injury += convert_dict[num_ch_last]
            count_injury -= count_injury_h

        # 对应上述第三种情况
        elif pattern_brackets_Chinese.findall(item) or pattern_brackets_English.findall(item):
            if '其中' in item:
                item_modify = item[:item.find('其中')]
            else:
                item_modify = item
            result = []
            # 匹配中文格式的括号
            if pattern_brackets_Chinese.findall(item_modify):
                result = pattern_brackets_Chinese.findall(item_modify)
            # 匹配英文格式的括号
            if pattern_brackets_English.findall(item_modify):
                result = pattern_brackets_English.findall(item_modify)
            for temp in result:
                count = 0
                for i in range(len(temp)):
                    if temp[i] in ['(', '（']:
                        # 检测括号中的内容
                        index_temp = i + 1
                        str_temp = ''
                        while index_temp < len(temp) and temp[index_temp] not in [
                                '）', ')']:
                            str_temp += temp[index_temp]
                            index_temp += 1
                        if str_temp not in illegal_list:
                            count += 1
                if count > 0:
                    # count>0说明在描述中有描述伤亡人数的句子，但是描述人员死亡或者受伤的情况非常多，没有普遍的规律性
                    # 需要将每种情况分别填写
                    if temp[-2] == '亡' or temp[-2] == '尸':
                        count_death = count
                    elif temp[-2] == '伤':
                        count_injury = count
                    elif temp[-2] == '踪':
                        if '确认死亡' in item:
                            count_death = count
                        else:
                            count_injury = count
                    elif temp.split('，')[-2][-1] == '亡':
                        count_death = count
                    elif temp.split('，')[-2][-1] == '伤':
                        count_injury = count
                    elif temp[-3: -1] == '救治' or temp[-3: -1] == '治疗':
                        count_injury = count
                    elif len(temp.split('，')) >= 3 and temp.split('，')[-3][-2:] == '救治':
                        count_injury = count
                    elif temp[-7:-1] == '已无生命体征':
                        count_death = count
                # 如果不满足上方的判断条件，说明句子中有括号，但是括号中描述的并不是受伤人的信息，真正描述受伤人数的句子还在后边
                # 检测一下关键句中是否有数字，如果有数字，应该是描述受伤人数的
                elif count == 0 and pattern_Arabic_numerals.findall(temp):
                    count += int(pattern_Arabic_numerals.findall(temp)[0])
                    count_injury = count

        # 除了上述三种有一定规律的其他情况
        # 如果经过以上几步检验之后没有伤亡人数，有可能是因为关键句的定位出现问题，真正描述伤亡人数的句子没有定位到
        if count_death == count_injury == 0 and ('中毒' in item or '症状' in item or '触电' in item or '撞' in item or
                                                 '接受检查' in item or '死亡' in item or '无生命体征' or '留院观察' in item
                                                 or '咬伤' in item):
            # 去掉其中的年月日等干扰项
            item_modify = re.sub(pattern_time, '', item, count=0)
            for sen in item_modify.split('，'):
                if sen != '':
                    if ('中毒' in sen or '症状' in sen or '撞' in sen or '接受检查' in sen or '留院观察' in sen or
                            '触电' in sen) and pattern_Arabic_numerals.findall(sen):
                        count_injury += int(
                            pattern_Arabic_numerals.findall(sen)[0])
                    if sen[-2:] == '目前' and sen[-4:] != '截至目前':
                        break
            if '咬伤' in item:
                count_injury += 1
            if count_injury == count_death == 0:
                if pattern_Arabic_numerals.findall(item_modify):
                    for num in pattern_Arabic_numerals.findall(item_modify):
                        if item_modify[item_modify.find(
                                num) + len(num)] == '名':
                            count_injury += int(num)
                            break
                if pattern_Chinese_numerals.findall(item_modify):
                    if pattern_Chinese_numerals.findall(item_modify)[0][1] in ['名', '工'] and (
                            '死亡' in item_modify or '无生命体征' in item_modify):
                        count_death += convert_dict[pattern_Chinese_numerals.findall(item_modify)[
                            0][0]]

            # 如果描述中有其中。。。。，是对伤亡人数更加具体的描述，需要对其中内的内容进行解析，解析其中。。。中描述的死亡人数
            # 如果死亡人数为0，说明在之前的描述中并没有提到死亡人数，在后面很有可能会出现关于死亡人数的描述
            # 如果死亡人数不为0，说明在之前的描述中提到了死亡人数，在后面很有可能是对之前死伤人数的补充描述
            if count_death == 0:
                death_num_modify = 0
                if pattern_qizhong.findall(item):
                    for sen_qizhong in pattern_qizhong.findall(item)[
                            0].split('，'):
                        if '死亡' in sen_qizhong:
                            if pattern_Arabic_numerals.findall(sen_qizhong):
                                death_num_modify += int(
                                    pattern_Arabic_numerals.findall(sen_qizhong)[0])
                            else:
                                death_num_modify += 1
                count_death += death_num_modify
                count_injury -= death_num_modify
        # 对括号后边的句子进行分析，可能后边的句子不包含括号，但是也是对受伤人员的描述
        if count_injury == 0:
            item_modify = re.sub(pattern_time, '', item, count=0)
            for sen in re.split('，|。|；', item_modify):
                temp_sen = sen.replace('\n', '')
                if '伤' in temp_sen:
                    if pattern_Arabic_numerals.findall(temp_sen):
                        if '重症' in temp_sen:
                            count_injury_h += int(
                                pattern_Arabic_numerals.findall(temp_sen)[0])
                        count_injury += int(
                            pattern_Arabic_numerals.findall(temp_sen)[0])
            count_injury -= count_injury_h

        uppdate_df.loc[uppdate_df.evt_id == row['evt_id'],
                       ['dead_pers_cnt',
                        'seri_injure_pers_cnt',
                        'minr_injure_pers_cnt']] = (count_death,
                                                    count_injury_h,
                                                    count_injury)
    #     num_analysis_result.append(
    #         [item, str(count_death), str(count_injury_h), str(count_injury)])
    # return resultlist


# 该函数用来检查分句中包含 顿号以及“及”，分句中描述的伤亡人数为顿号或者“及”个数加1
def find_all_num(sentence: str, label: list):
    count = 0
    for s in sentence:
        if s in label:
            count += 1
    return count + 1


# 该函数用来检查分句中包含 顿号、“及”、“和” ，分句中如果包含阿拉伯数字或者中文数字需要单独考虑
def find_all_num_update(
        sentence: str,
        label: list,
        pattern_num: object,
        pattern_num_ch: object,
        illness_list: list):
    convert_dict = {'一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6,
                    '七': 7, '八': 8, '九': 9, '十': 10}
    count = 0
    count_h = 0
    sen_temp = re.split('|'.join(label), sentence)
    for item in sen_temp:
        has_check = True
        if item:
            # 有些疾病症状也是通过顿号连接的，这样就会造成最后的统计结果中伤亡人数变多，如果症状在里边，就去掉
            for ill in illness_list:
                if ill in item:
                    has_check = False
                    break
            if has_check:
                # 如果在分句中匹配不到阿拉伯数字和中文数字，说明只有一个人
                if not pattern_num.findall(
                        item) and not pattern_num_ch.findall(item):
                    count += 1
                else:
                    if pattern_num.findall(item):
                        if '伤' in item:
                            count_h += int(pattern_num.findall(item)[0])
                        else:
                            count += int(pattern_num.findall(item)[0])
                    if pattern_num_ch.findall(item):
                        count += convert_dict[pattern_num_ch.findall(item)[
                            0][0]]
    return count, count_h


def convert_str_to_list(string: str):
    list_modify = []
    for item in string.split(','):
        list_modify.append(item.strip())
    return list_modify


def main():
    data = pd.read_sql(
        'select * from biz_current.a_me_emcy_evt_info',
        con=emergency_event.pg_con)

    #
    # # 用来存储全部的参数
    # all_parameters = {}
    # # 读取配置json文件，将需要的参数赋值给对应的变量
    # with open('D:\\yxyFile\\project\\python project\\emergency_event\\parameter.json', 'r', encoding='utf-8') as f:
    #     all_parameters = json.load(f)
    # # 突发事件数据路径
    # filepath = all_parameters['filepath']
    # # 紧急事件描述对应的sheet名称
    # emergency_sheet_name = all_parameters['emergency_sheetname']
    # # 伤亡人数解析结果输出路径
    # output_result_path = all_parameters['output_result_path']
    # 可能存在人员伤亡的突发事件类别
    c_first = convert_str_to_list(rules['c_first'])
    # 可能存在人员伤亡的突发事件类型
    c_second = convert_str_to_list(rules['c_second'])
    # 用来排除描述症状信息的子句
    illness_list = convert_str_to_list(rules['illness_list'])
    # 用来排除描述非人的括号内容
    illegal_list = convert_str_to_list(rules['illegal_list'])
    # 可能出现人员伤亡的正则表达式
    pattern_possible_str = rules['pattern_possible_str']
    # 在可能造成人员伤亡的事件中，并没有出现人员伤亡的正则表达式
    pattern_no_hurt_str = rules['pattern_no_hurt_str']
    # 第一种情况，事故描述中有 造成... 这样格式的描述，在这样描述中通常描述了事故伤亡的人数
    pattern_first_situation_str = rules['pattern_first_situation_str']
    # 第二种情况，事故描述为 导致/致....死亡/受伤
    pattern_second_situation_str = rules['pattern_second_situation_str']
    # 匹配中文格式的括号
    pattern_brackets_Chinese_str = rules['pattern_brackets_Chinese_str']
    # 匹配英文格式的括号
    pattern_brackets_English_str = rules['pattern_brackets_English_str']
    # 用来匹配在第一种情况和第二种情况中的 造成。。。/ 导致。。。 描述中的括号内部的内容，在解析过程中需要将括号中的内容先去掉，避免造成干扰
    pattern_discribe_str = rules['pattern_discribe_str']
    # 匹配阿拉伯数字
    pattern_Arabic_numerals_str = rules['pattern_Arabic_numerals_str']
    # 匹配中文数字
    pattern_Chinese_numerals_str = rules['pattern_Chinese_numerals_str']
    # 匹配描述事件发生时间的数字格式，这些数字会对解析过程产生干扰，需要去除
    pattern_time_str = rules['pattern_time_str']
    # 匹配其中。。。。，在其中里边的描述可能会包含有 无明显外伤之类的描述，需要将无明显外伤这类的人数去除
    pattern_qizhong_str = rules['pattern_qizhong_str']
    # 设置描述中出现的连接词列表
    label_list = convert_str_to_list(rules['label_list'])

    # 对产生人员伤亡的事件的描述进行解析，得到死亡人数和受伤人数，并将结果写入到txt中
    get_people_hurt_num(
        data,
        illness_list,
        illegal_list,
        pattern_first_situation_str,
        pattern_second_situation_str,
        pattern_brackets_Chinese_str,
        pattern_brackets_English_str,
        pattern_discribe_str,
        pattern_Arabic_numerals_str,
        pattern_Chinese_numerals_str,
        pattern_time_str,
        pattern_qizhong_str,
        label_list)

    print(data)
    # 将结果导出为xlsx
    # result_to_xlsx(num_analysis_result, output_xlsx_path)
    # 将结果到处为txt
    # write_into_txt(output_result_path, num_analysis_result)
    data2 = data.to_numpy()
    tab_name = 'biz_current.tab2019'
    sql = f"""insert into {tab_name} values ({','.join(["%s"]*len(data.keys()))});"""
    emergency_event.extras.execute_batch(
        emergency_event.pg_cor, sql, data2, page_size=2000)
    emergency_event.pg_con.commit()


if __name__ == '__main__':
    main()
