import sys
import os
def print_class(class_name, class_set):
    print(class_name+":")
    for x in class_set:
        print("\t"+x)

def main(out_dir, pos_file, neg_file, thresh):
    pos_set = []
    neg_set = []
    res_pos = []
    res_no_data = []
    res_neg = []
    for line in open(pos_file, "r").readlines():
        pos_set.append(line.replace('\r\n',''))
    for line in open(neg_file, "r").readlines():
        neg_set.append(line.replace('\r\n',''))
    for suffix in ["00","01", "02"]:
        filename = os.path.join(out_dir,"part-r-000"+suffix)
        for line in open(filename, "r").readlines():
            k_v = line.split('\t')
            sim = k_v[1].strip('\n')
            compatible_line = k_v[0].replace("Y X", "Y\tX").replace("Y Y", "Y\tY")
            if sim == 'NaN':
                res_neg.append(compatible_line)
            sim_f = float(sim)
            if sim_f == 0:
                res_no_data.append(compatible_line)
            elif sim_f >= thresh:
                res_pos.append(compatible_line)
            else:
                res_neg.append(compatible_line)

    true_positive = filter(lambda x: x in pos_set, res_pos)
    true_negative = filter(lambda x: x in neg_set, res_neg)
    false_positive = filter(lambda x: x in neg_set, res_pos)
    false_negative = filter(lambda x: x in pos_set, res_neg)
    print_class("true_positive", true_positive)
    print_class("true_negative", true_negative)
    print_class("false_positive", false_positive)
    print_class("false_negative", false_negative)
    print("##################")
    print("precision: "+str(float(len(true_positive)) / float(len(true_positive+false_positive))))
    print("recall: "+str(float(len(true_positive)) / float(len(true_positive+false_negative))))





if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4]))
