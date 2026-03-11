from utils.alias_cache_util import get_alias_cache
from utils.label_cache_util import get_label_cache
import re

def normalize(input_str):
    return re.sub(r'[^a-zA-Z0-9]', '', input_str).lower()

def compare_a_pred_golden_pair(golden, pred):
    #两边都是一个n维的tuple，逐个比较吧
    if len(golden) != len(pred):
        return False
    else:
        for i in range(0, len(golden)):
            if re.fullmatch("Q\d+", golden[i]):    #qid
                if not verify(golden[i], pred[i]):
                    return False
            elif golden[i] == "TRUE":
                if not (str(pred[i]).lower() == "true"):
                    return False
            elif golden[i] == "FALSE":
                if not (str(pred[i]).lower() == "false"):
                    return False
            else:   #时间点
                if isinstance(pred[i], int) or isinstance(pred[i], float):
                    pred = str(pred)
                if not str(pred) == str(golden):
                    return False
        return True


def verify(qid: str, ans: str):
    golden_answers = get_alias_cache(qid)
    golden_answers += (get_label_cache(qid))
    normalized_golden_answers = [normalize(answer) for answer in golden_answers]
    normalized_ans = normalize(ans)
    return normalized_ans in normalized_golden_answers





if __name__ == '__main__':
    print(verify("Q76", "Barack Hussein Obama II"))