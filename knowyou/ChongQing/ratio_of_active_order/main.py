import matplotlib.pyplot as plt

if __name__ == '__main__':
    listDict = []

    file_object = open('ratio_active_order_result.bcp', 'r')

    try:
        for line in file_object:
            l = line.split("\t")
            dict = {'video_cnt': l[0], 'total_cnt': l[1], 'order_cnt': l[2], 'ratio': l[3]}
            listDict.append(dict)
    finally:
        file_object.close()

    active = []
    ratios = []

    total = 0
    order = 0
    sumRatio = 0
    lens = len(listDict)

    for dic in listDict:
        video_cnt = int(dic["video_cnt"])
        total_cnt = int(dic["total_cnt"])
        order_cnt = int(dic["order_cnt"])
        ratio = float(dic["ratio"])

        if video_cnt == 1:
            active.append("1")
            ratios.append(ratio)
            continue

        if video_cnt <= 3:
            if "2~3" not in active:
                active.append("2~3")
            # sumRatio += ratio
            total += total_cnt
            order += order_cnt
            if video_cnt == 3:
                # if sumRatio == 0:
                #     sumRatio = -0.01

                sumRatio = float(order)/float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue

        if video_cnt <= 6:
            if "4~6" not in active:
                active.append("4~6")
            # sumRatio += ratio
            total += total_cnt
            order += order_cnt
            if video_cnt == 6:
                # if sumRatio == 0:
                #     sumRatio = -0.01
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue

        if video_cnt <= 10:
            if "7~10" not in active:
                active.append("7~10")
            # sumRatio += ratio
            total += total_cnt
            order += order_cnt
            if video_cnt == 10:
                # if sumRatio == 0:
                #     sumRatio = -0.01
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue

        if video_cnt <= 14:
            if "11~14" not in active:
                active.append("11~14")
            # sumRatio += ratio
            total += total_cnt
            order += order_cnt
            if video_cnt == 14:
                # if sumRatio == 0:
                #     sumRatio = -0.01
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue

        if video_cnt <= 20:
            if "15~20" not in active:
                active.append("15~20")
            # sumRatio += ratio
            total += total_cnt
            order += order_cnt
            if video_cnt == 20:
                # if sumRatio == 0:
                #     sumRatio = -0.01
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue

        if video_cnt <= 30:
            if "21~30" not in active:
                active.append("21~30")
            # sumRatio += ratio
            total += total_cnt
            order += order_cnt
            if video_cnt == 30:
                # if sumRatio == 0:
                #     sumRatio = -0.01
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue

        if video_cnt <= 50:
            if "31~50" not in active:
                active.append("31~50")
            # sumRatio += ratio
            total += total_cnt
            order += order_cnt
            if video_cnt == 50:
                # if sumRatio == 0:
                #     sumRatio = -0.01
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue

        if video_cnt <= 80:
            if "51~80" not in active:
                active.append("51~80")
            # sumRatio += ratio
            total += total_cnt
            order += order_cnt
            if video_cnt == 80:
                # if sumRatio == 0:
                #     sumRatio = -0.01
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue

        if video_cnt <= 120:
            if "81~120" not in active:
                active.append("81~120")
            # sumRatio += ratio
            total += total_cnt
            order += order_cnt
            if video_cnt == 120:
                # if sumRatio == 0:
                #     sumRatio = -0.01
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue

        if video_cnt <= 170:
            if "121~170" not in active:
                active.append("121~170")
            # sumRatio += ratio
            total += total_cnt
            order += order_cnt
            if video_cnt == 170:
                # if sumRatio == 0:
                #     sumRatio = -0.01
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue

        if video_cnt <= 230:
            if "171~230" not in active:
                active.append("171~230")
            # sumRatio += ratio
            total += total_cnt
            order += order_cnt
            if video_cnt == 230:
                # if sumRatio == 0:
                #     sumRatio = -0.01
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue

        if video_cnt <= 300:
            if "231~300" not in active:
                active.append("231~300")
            # sumRatio += ratio
            total += total_cnt
            order += order_cnt
            if video_cnt == 300:
                # if sumRatio == 0:
                #     sumRatio = -0.01
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue

        if video_cnt <= 500:
            if "301~500" not in active:
                active.append("301~500")
            total += total_cnt
            order += order_cnt
            if video_cnt == 500:
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue
        if video_cnt <= 6287:
            if "501~6287" not in active:
                active.append("501~6287")
            total += total_cnt
            order += order_cnt
            if video_cnt == 6287:
                sumRatio = float(order) / float(total)
                ratios.append(sumRatio)
                sumRatio = 0
                total = 0
                order = 0
            continue


    plt.rcParams['font.sans-serif']=['SimHei']    # 处理中文无法正常显示的问题
    plt.rcParams['axes.unicode_minus'] = False    # 负号显示

    plt.xlabel("观看节目数")
    plt.ylabel("订购率")

    fig = plt.figure()
    plt.bar(active, ratios, 0.4, color="pink")
    plt.title("用户活跃度与订购率关系图")
    # plt.plot(active, ratios)
    plt.show()


