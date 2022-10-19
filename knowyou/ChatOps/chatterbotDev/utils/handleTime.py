import re
import pkuseg
import jieba.posseg as pseg
import datetime
import time

from dateutil.parser import parse

##
# NLP - NER


UTIL_CN_NUM = {
    '零': 0, '〇': 0, '一': 1, '二': 2, '两': 2, '三': 3,
    '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
    '0': 0, '1': 1, '2': 2, '3': 3, '4': 4,
    '5': 5, '6': 6, '7': 7, '8': 8, '9': 9
}

UTIL_CN_UNIT = {'十': 10, '百': 100, '千': 1000, '万': 10000}


def cn_2_digital(src):
    """
    Change Chinese to number
    :param src:
    :return:
    """
    if src == "":
        return None
    # 是否以数字开始
    m = re.match(r"\d+", src)
    if m:
        return int(m.group(0))

    rsl = 0
    unit = 1
    # 倒序遍历src，倒序从个位数进行获取
    for item in src[::-1]:
        if item in UTIL_CN_UNIT.keys():
            unit = UTIL_CN_UNIT.get(item)
        elif item in UTIL_CN_NUM.keys():
            num = UTIL_CN_NUM.get(item)
            rsl += num * unit
        else:
            return None

    if rsl < unit:
        rsl += unit

    return rsl


def cn_year_2_digital(year):
    """
    Change and form year of Chinese to number
    :param year:
    :return:
    """
    res = ''
    for item in year:
        if item in UTIL_CN_NUM.keys():
            res += str(UTIL_CN_NUM.get(item))
        else:
            res += item

    m = re.match(r"\d+", res)
    if m:
        if len(m.group(0)) == 2:
            return int(datetime.date.today().year / 100) * 100 + int(m.group(0))
        else:
            return int(m.group(0))
    else:
        return None


def parser_datetime(msg):
    """
    Parser and handle time through by regexp expression
    :param msg:
    :return:
    """
    if msg is None or len(msg) == 0:
        return None

    try:
        dt = parse(msg, fuzzy=True)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        m = re.match(
            # r"([0-9〇零一二两三四五六七八九十]+[\.年]?)?([ 0-9〇零一二两三四五六七八九十]+)?[\.月]?"
            # r"([ 0-9〇零一二两三四五六七八九十]+)?[\.号日]?([ 上中下午晚早]+)?"
            # r"([ 0-9〇零一二两三四五六七八九十百]+)?[\.:点时]?([ 0-9〇零一二三四五六七八九十百]+)?[\.:分钟]*"
            # r"([ 0-9〇零一二三四五六七八九十百]+)?秒?",

            r"([0-9〇零一二两三四五六七八九十]+年)? *([0-9〇零一二两三四五六七八九十]+月)? *"
            r"([0-9〇零一二两三四五六七八九十]+[号日])? *([上中下午晚早]+)? *"
            r"([0-9〇零一二两三四五六七八九十百]+[点:.时])? *([0-9〇零一二三四五六七八九十百]+分?)? *"
            r"([0-9〇零一二三四五六七八九十百]+秒?)?",
            msg
        )
        if m and m.group(0) is not None:
            res = {
                "year": m.group(1),
                "month": m.group(2),
                "day": m.group(3),
                "hour": m.group(5) if m.group(5) is not None else '00',
                "minute": m.group(6) if m.group(6) is not None else '00',
                "second": m.group(7) if m.group(7) is not None else '00',
            }
            params = {}

            for name in res:
                if res[name] is not None and len(res[name]) != 0:
                    # print('--text-- {} : {}', name, res[name])
                    tmp = None
                    if name == 'year':
                        # 删掉最后的字符
                        tmp = cn_year_2_digital(res[name][:-1])
                    else:
                        # tmp = cn_2_digital(res[name])
                        tmp = cn_2_digital(res[name][:-1])

                    if tmp is not None:
                        params[name] = int(tmp)

            # 整个时间通过传入字典参数替换
            target_date = datetime.datetime.today().replace(**params)

            # 判断并处理am还是pm
            is_pm = m.group(4)
            if is_pm is not None:
                if is_pm == u'下午' or is_pm == u'晚上' or is_pm == u'中午':
                    hour = target_date.time().hour
                    if hour < 12:
                        target_date = target_date.replace(hour=hour + 12)
            return target_date.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return None


def check_time_valid(word):
    m = re.match(r'\d+$', word)
    if m:
        # 清洗掉长度小于等于6的纯数字，非准确日期
        if len(word) <= 6:
            return None

    word1 = re.sub(r'[号|日]\d+$', '日', word)

    if word1 != word:
        return check_time_valid(word1)
    else:
        return word1


def time_extract(text):
    """
    Use jieba to segment the input statement, then based on the part of speech select time and number words
    :param text:
    :return:
    """
    time_res = []
    word = ''
    key_date = {'前天': -2, '前日': -2, '昨天': -1, '昨日': -1, '今天': 0, '今日': 0, '明天': 1, '明日': 1, '后天': 2}

    # seg = pkuseg.pkuseg(postag=True)
    # for k, v in seg.cut(text):
    for k, v in pseg.cut(text):
        if k in key_date:
            if word != '':
                time_res.append(word)

            word = (datetime.datetime.today() + datetime.timedelta(days=key_date.get(k, 0))).strftime(
                '%Y{y}%m{m}%d{d}').format(y='年', m='月', d='日')
        elif word != '':
            if v in ['t', 'm']:
                word += k
            else:
                time_res.append(word)
                word = ''
        elif v in ['t', 'm']:
            word = k

    if word != '':
        time_res.append(word)

    # lambda函数作为参数用于传递给其他函数指定过滤条件元素的列表
    result = list(filter(lambda x: x is not None, [check_time_valid(w) for w in time_res]))
    final_res = [parser_datetime(m) for m in result]

    return [x for x in final_res if x is not None]


def extract_time_interval(text):
    """
    Obtain time interval from result list which is handled from text
    :param text:
    :return:
    """
    data = {}

    currentDay = datetime.date.today()
    currentDate = currentDay.strftime("%Y-%m-%d")
    todayAt0Clock = currentDay.strftime("%Y-%m-%d 00:00:00")
    todayAtCurrentTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    time_list = time_extract(text)

    if len(time_list) == 0:
        data['startTime'] = todayAt0Clock
        data['endTime'] = todayAtCurrentTime
    elif len(time_list) == 1:
        get_time = time_list[0]
        if todayAt0Clock.split(' ')[0] != get_time.split(' ')[0]:
            if get_time.split(' ')[1] == '00:00:00':
                data['startTime'] = get_time
                data['endTime'] = get_time.replace('00:00:00', '23:59:59')
            else:
                data['startTime'] = re.sub(r'\d+:\d+:\d+', '00:00:00', get_time)
                data['endTime'] = get_time
        else:
            if get_time.split(' ')[1] == '00:00:00':
                data['startTime'] = get_time
                data['endTime'] = todayAtCurrentTime
            else:
                data['startTime'] = todayAt0Clock
                data['endTime'] = get_time
    elif len(time_list) == 2:
        if time_list[0] < time_list[1]:
            data['startTime'] = time_list[0]
            data['endTime'] = time_list[1]
        else:
            data['startTime'] = time_list[1]
            data['endTime'] = time_list[0]
    else:
        data['startTime'] = min(time_list)
        data['endTime'] = max(time_list)

    return data


def extract_date_day(text, flag):
    """
    Obtain the date. If the flag is True, then the date with %H%M%S.
    :param text:
    :param flag:
    :return:
    """

    currentDay = datetime.date.today()
    todayAt0Clock = currentDay.strftime("%Y-%m-%d 00:00:00")

    time_list = time_extract(text)

    if len(time_list) == 0:
        result = todayAt0Clock
    elif len(time_list) == 1:
        result = time_list[0]
    else:
        result = time_list[-1]

    if flag:
        return result
    else:
        return result.split(' ')[0]


def extract_oneday_0to24(text):
    text.replace('-', '.')


if __name__ == '__main__':
    print(time_extract('2022.03.14'))
    print(extract_time_interval('2022.03.14'))
    # print(extract_time_interval('查询昨日告警'))
    # print(extract_time_interval('查询1月18告警'))

    # print(extract_date_day('查询今日告警', True))
    # print(extract_date_day('查询昨日告警', True))
    # print(extract_date_day('查询1月18告警', False))
    # print(extract_date_day('上海天气', False))
    # print(cn_2_digital("二十二"))
    # print(cn_2_digital("一百一十三"))
    # print(cn_2_digital("十五"))
    # print(cn_year_2_digital("二零二二年"))
    # print(cn_year_2_digital("二二年"))
    # print(cn_year_2_digital("2022年"))
    # print(cn_year_2_digital("二2"))
    # print(cn_year_2_digital("你说咱们22年"))
    # print(cn_2_digital("前日到明日"))
    # m = re.match(
    #     r"([0-9〇零一二两三四五六七八九十]+[\.年]?)?([0-9〇零一二两三四五六七八九十]+)?[\.月]?([0-9〇零一二两三四五六七八九十]+)?[\.号日]?([上中下午晚早]+)?([0-9〇零一二两三四五六七八九十百]+)?[\.:点时]?([0-9〇零一二三四五六七八九十百]+)?[\.:分钟]*([0-9〇零一二三四五六七八九十百]+)?秒?",
    #     '二〇二〇年零一月19号下午三点十五分钟十五秒'
    # )
    # print(m)
    # print(m.group(0))
    # print(m.group(1))
    # print(m.group(2))
    # print(m.group(3))
    # print(m.group(4))
    # print(m.group(5))
    # print(m.group(6))
    # print(m.group(7))

    # print(parser_datetime('二零二零年一月19日下午三点十五分15秒'))
    # print(parser_datetime('二〇二〇年零一月19日下午三点十五分15秒'))
    # print(parser_datetime('二零二〇年零一月十九日下午三点十五分15秒'))
    # print(parser_datetime('二零二零年一月 19日下午三点十五分15秒'))
    # print(parser_datetime('二零二零年一月19日 下午三点十五分15秒'))
    # print(parser_datetime('二〇二〇年一月19号下午三点十五分钟十五秒'))
    # print(parser_datetime('二〇二〇年一月19号下午三点十五分十五'))

    # print(datetime.datetime.today())
    #
    # text_1 = '我昨日晚上十点喂的斯芬克斯'
    # print(text_1, time_extract(text_1), sep=':')
    #
    # text0 = '鹤翔今天没出来他肯定难受憋得慌'
    # print(text0, time_extract(text0), sep=':')
    #
    # text1 = '翀明天上午十点必去WC'
    # print(text1, time_extract(text1), sep=':')
    #
    # text2 = '预定28号的房间'
    # print(text2, time_extract(text2), sep=':')
    #
    # text3 = '春节从29号下午4点直到2月5号'
    # print(text3, time_extract(text3), sep=':')
    #
    # text3 = '春节从29日下午4点直到2月5号'
    # print(text3, time_extract(text3), sep=':')
    #
    # text3 = '29号下午4点到2月5号早上八点'
    # print(text3, time_extract(text3), sep=':')
    #
    # text4 = '我要预订今天到30日的房间'
    # print(text4, time_extract(text4), sep=':')
    #
    # text5 = '前日到后日'
    # print(text5, time_extract(text5), sep=':')
    #
    # text_1 = '我昨日来的'
    # print(text_1, time_extract(text_1), sep=':')
    #
    # text0 = '我今天到的'
    # print(text0, time_extract(text0), sep=':')
    #
    # text1 = '我要住到明天下午三点'
    # print(text1, time_extract(text1), sep=':')
    #
    # text2 = '预定28号的房间'
    # print(text2, time_extract(text2), sep=':')
    #
    # text3 = '我要从26号下午4点住到11月2号'
    # print(text3, time_extract(text3), sep=':')
    #
    # text4 = '我要预订今天到30日的房间'
    # print(text4, time_extract(text4), sep=':')
    #
    # text = '查询昨日A类割接'
    # print(text, time_extract(text), sep=':')
