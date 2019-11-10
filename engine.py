import re
import sys
import copy
import sqlparse as sqlp
from Data_container import Database, Table

dbase = Database('metadata.txt')
join_var = []


def col_index(col, col_list):
    if col in col_list:
        return col_list.index(col)
    else:
        split_list = [u.split('.') for u in col_list]
        col_ind = -1
        for i in range(len(split_list)):
            if col.strip() == split_list[i][1]:
                if col_ind == -1:
                    col_ind = i
                else:
                    print(
                        'SQLEngineError: Presence of duplicate column. Specify in form table.col')
                    exit()
        if col_ind == -1:
            print(
                'SQLEngineError: Non-existent column or presence of agregate function with columns')
            exit()
        return col_ind


def multiple_agg(clauses, joints_table):
    clause_split = [u.split('(') for u in clauses]
    for aggr_handl in clause_split:
        agg_col = aggr_handl[1].strip(')').split()
        agg_func = aggr_handl[0].strip()

        if len(agg_col) < 1 or len(agg_col) > 2:
            print('SQLEngineError: Invalid statement')
            exit()

        if len(agg_col) == 2 and agg_col[0] != 'distinct':
            print('SQLEngineError: Invalid statement')
            exit()

        d_agg = 1 if agg_col[0] == 'distinct' else 0

        ind = col_index(agg_col[d_agg], joints_table.cols)

        if agg_func == 'max':
            max_num = None
            for row in joints_table.data:
                if max_num == None or row[ind] > max_num:
                    max_num = row[ind]
            print('max(' + joints_table.cols[ind] + ')')
            print(str(max_num))

        elif agg_func == 'min':
            min_num = None
            for row in joints_table.data:
                if min_num == None or row[ind] < min_num:
                    min_num = row[ind]

            print('min(' + joints_table.cols[ind] + ')')
            print(str(min_num))

        elif agg_func == 'sum':
            fin_data = [row[ind] for row in joints_table.data]
            sum_ = sum(fin_data) if d_agg == 0 else sum(set(fin_data))
            print('sum(' + joints_table.cols[ind] + ')')
            print(str(sum_))
        elif agg_func == 'average':
            fin_data = [row[ind] for row in joints_table.data]
            sum_ = sum(fin_data) if d_agg == 0 else sum(set(fin_data))
            print('average(' + joints_table.cols[ind] + ')')
            l = len(fin_data) if d_agg == 0 else len((set(fin_data)))
            print(str(sum_/l))
        else:
            print('SQLEngineError: Invalid aggregate statement')
            exit()


def display_result(select_clause, joints_table, disinct_flag=0):
    if len(select_clause) == 0:
        print('SQLEngineError: No statements for SELECT given')
        exit()

    clause_list = [u.strip() for u in select_clause.split(',')]

    if len(join_var) > 2:
        print('SQLEngineError: Only one join condition supported by engine')
        exit()

    if len(clause_list) > 1:
        g = [u.split('(') for u in clause_list]
        mult_agg = 1

        for row in g:
            if len(row) != 2:
                mult_agg = 0
                break

        if mult_agg == 1:
            multiple_agg(clause_list, joints_table)
            exit()

        if '*' in clause_list:
            if len(join_var) > 0 and join_var[0][0] != join_var[0][1]:
                exclude_ind = joints_table.cols.index(join_var[0][1])

                out_cols = copy.copy(joints_table.cols)
                out_cols.remove(join_var[0][1])
                fin_data = copy.deepcopy(joints_table.data)

                for i in range(len(fin_data)):
                    fin_data[i].pop(exclude_ind)

                print(','.join(out_cols))

                if disinct_flag:
                    out_data = []
                    for row in fin_data:
                        if row not in out_data:
                            out_data.append(row)
                    for row in out_data:
                        print(','.join([str(u) for u in row]))
                else:
                    for row in fin_data:
                        print(','.join([str(u) for u in row]))
            else:
                if disinct_flag:
                    out_data = []
                    for row in joints_table.data:
                        if row not in out_data:
                            out_data.append(row)
                    for row in out_data:
                        print(','.join([str(u) for u in row]))
                else:
                    for row in joints_table.data:
                        print(','.join([str(u) for u in row]))

        else:
            col_ind_list = [col_index(u, joints_table.cols)
                            for u in clause_list]
            col_ind_list.sort()

            out_cols = [joints_table.cols[i] for i in col_ind_list]
            fin_data = copy.deepcopy(joints_table.data)
            fin_data = [[col[i] for i in col_ind_list] for col in fin_data]

            print(','.join(out_cols))

            if disinct_flag:
                out_data = []
                for row in fin_data:
                    if row not in out_data:
                        out_data.append(row)
                for row in out_data:
                    print(','.join([str(u) for u in row]))
            else:
                for row in fin_data:
                    print(','.join([str(u) for u in row]))

    else:
        if clause_list[0] == '*':
            if len(join_var) > 0:
                exclude_ind = joints_table.cols.index(join_var[0][1])

                out_cols = copy.copy(joints_table.cols)
                out_cols.remove(join_var[0][1])
                fin_data = copy.deepcopy(joints_table.data)

                for i in range(len(fin_data)):
                    fin_data[i].pop(exclude_ind)

                print(','.join(out_cols))

                if disinct_flag:
                    out_data = []
                    for row in fin_data:
                        if row not in out_data:
                            out_data.append(row)
                    for row in out_data:
                        print(','.join([str(u) for u in row]))
                else:
                    for row in fin_data:
                        print(','.join([str(u) for u in row]))
            else:
                out_cols = copy.copy(joints_table.cols)
                print(','.join(out_cols))

                fin_data = copy.deepcopy(joints_table.data)
                if disinct_flag:
                    out_data = []
                    for row in fin_data:
                        if row not in out_data:
                            out_data.append(row)
                    for row in out_data:
                        print(','.join([str(u) for u in row]))
                else:
                    for row in fin_data:
                        print(','.join([str(u) for u in row]))

        else:
            aggr_handl = clause_list[0].split('(')
            if len(aggr_handl) == 1:
                # just a column
                ind = col_index(aggr_handl[0].strip(), joints_table.cols)
                out_cols = joints_table.cols[ind]
                print(out_cols)
                fin_data = []

                for row in joints_table.data:
                    if disinct_flag:
                        if row[ind] not in fin_data:
                            fin_data.append(row[ind])
                    else:
                        fin_data.append(row[ind])

                print('\n'.join([str(u) for u in fin_data]))

            else:
                agg_col = aggr_handl[1].strip(')').split()
                agg_func = aggr_handl[0].strip()

                if len(agg_col) < 1 or len(agg_col) > 2:
                    print('SQLEngineError: Invalid statement')
                    exit()

                if len(agg_col) == 2 and agg_col[0] != 'distinct':
                    print('SQLEngineError: Invalid statement')
                    exit()

                d_agg = 1 if agg_col[0] == 'distinct' else 0

                ind = col_index(agg_col[d_agg], joints_table.cols)

                if agg_func == 'max':
                    max_num = None
                    for row in joints_table.data:
                        if max_num == None or row[ind] > max_num:
                            max_num = row[ind]
                    print('max(' + joints_table.cols[ind] + ')')
                    print(str(max_num))

                elif agg_func == 'min':
                    min_num = None
                    for row in joints_table.data:
                        if min_num == None or row[ind] < min_num:
                            min_num = row[ind]

                    print('min(' + joints_table.cols[ind] + ')')
                    print(str(min_num))

                elif agg_func == 'sum':
                    fin_data = [row[ind] for row in joints_table.data]
                    sum_ = sum(fin_data) if d_agg == 0 else sum(set(fin_data))
                    print('sum(' + joints_table.cols[ind] + ')')
                    print(str(sum_))
                elif agg_func == 'average':
                    fin_data = [row[ind] for row in joints_table.data]
                    sum_ = sum(fin_data) if d_agg == 0 else sum(set(fin_data))
                    print('average(' + joints_table.cols[ind] + ')')
                    l = len(fin_data) if d_agg == 0 else len((set(fin_data)))
                    print(str(sum_/l))
                else:
                    print('SQLEngineError: Invalid aggregate statement')
                    exit()


def sing_table(table_set):
    keyset = sorted(table_set.keys())
    columns = []

    if len(keyset) == 0:
        # no tables
        print("SQLEngineError: Invalid table count")
        exit()

    for table in keyset:
        columns += [table+'.' + col for col in table_set[table].cols]

    table_singular = Table('final', columns)

    data = copy.deepcopy(table_set[keyset[0]].data)
    for i in range(1, len(keyset)):
        temp_data = copy.deepcopy(data)
        data = []
        for row0 in temp_data:
            for row1 in table_set[keyset[i]].data:
                data.append(row0+row1)

    table_singular.push_data(data)
    return table_singular


def eval_cond(tables, condition):
    keyset = sorted(tables.keys())
    joint_data = []

    full_table = sing_table(tables)

    sign = re.findall(r'[<>=]+', condition)
    if sign[0] not in ['<', '>', '<=', '>=', '=']:
        print('SQLEngineError: Invalid condition')
        exit()
    cond_var = [u.strip() for u in re.split(r'[<>=]+', condition)]

    if not len(sign) or len(cond_var) != 2:
        print(condition)
        print('SQLEngineError: Invalid condition')
        exit()

    cond_type = [0, 0]

    # 0 for type col_name,
    # 1 for type tabe_name.col_name
    # 2 for an integer type

    col_belongs_to = [None, None]
    # If the condition clause is a column,
    # which table does it belong to?

    cond1 = cond_var[0].split('.')
    cond2 = cond_var[1].split('.')

    if len(cond1) == 2:
        if cond1[0] not in keyset:
            print('SQLEngineError: Non-existent table')
            exit()

        cond_type[0] = 1
        if cond1[1] in tables[cond1[0]].cols:
            col_belongs_to[0] = cond1[0]
        else:
            print('SQLEngineError: Non-existent column')
            exit()

    if len(cond2) == 2:
        if cond2[0] not in keyset:
            print('SQLEngineError: Non-existent table')
            exit()

        cond_type[1] = 1
        if cond2[1] in tables[cond2[0]].cols:
            col_belongs_to[1] = cond2[0]
        else:
            print('SQLEngineError: Non-existent column')
            exit()

    try:
        if cond_type[0] != 1:
            cond1[0] = int(cond1[0])
            cond_type[0] = 2
    except:
        cond_type[0] = 0

    try:
        if cond_type[1] != 1:
            cond2[0] = int(cond2[0])
            cond_type[1] = 2
    except:
        cond_type[1] = 0

    if cond_type[0] == 0:
        for key in keyset:
            if key+'.'+cond1[0] in full_table.cols:
                if col_belongs_to[0] == None:
                    col_belongs_to[0] = key
                else:
                    print(
                        'SQLEngineError: Presence of duplicate column. Specify in form table.col')
                    exit()

        if col_belongs_to[0] == None:
            print('SQLEngineError: Non-existent column')
            exit()

    if cond_type[1] == 0:
        for key in keyset:
            if key+'.'+cond2[0] in full_table.cols:
                if col_belongs_to[1] == None:
                    col_belongs_to[1] = key
                else:
                    print(
                        'SQLEngineError: Presence of duplicate column. Specify in form table.col')
                    exit()

        if col_belongs_to[1] == None:
            print('SQLEngineError: Non-existent column')
            exit()

    cond_col = [None, None]
    cond_col[0] = cond1[cond_type[0]] if cond_type[0] != 2 else None
    cond_col[1] = cond2[cond_type[1]] if cond_type[1] != 2 else None

    cond_index = [-1, -1]

    if cond_type[0] < 2:
        cond_index[0] = full_table.cols.index(
            col_belongs_to[0]+'.'+cond_col[0])

    if cond_type[1] < 2:
        cond_index[1] = full_table.cols.index(
            col_belongs_to[1]+'.'+cond_col[1])

    if sign[0] == '=':
        sign[0] = '=='

    for row in full_table.data:
        if col_belongs_to[0] != None:
            v1 = row[cond_index[0]]
        else:
            v1 = cond1[0]

        if col_belongs_to[1] != None:
            v2 = row[cond_index[1]]
        else:
            v2 = cond2[0]

        bool_var = eval(str(v1) + sign[0] + str(v2))
        if bool_var:
            joint_data.append(row)

    fin_table = Table("final", full_table.cols)
    fin_table.push_data(joint_data)

    # checking for a join variable
    if cond_type[0] < 2 and cond_type[1] < 2 and sign[0] == '==':
        c1 = col_belongs_to[0]+'.'+cond_col[0]
        c2 = col_belongs_to[1]+'.'+cond_col[1]
        # print((c1, c2))
        join_var.append(tuple((c1, c2)))
    return fin_table


def where_filter(tables, conds):
    cond_list = conds.split(' ')[1:]
    and_or_flag = -1
    # -1 if no AND and no OR, 0 if AND, 1 if OR

    if 'and' in cond_list:
        and_or_flag = 0
    if 'or' in cond_list:
        and_or_flag = 1

    if and_or_flag == -1:
        cond_set = ' '.join(cond_list).strip(' ;')
        filter_table = eval_cond(tables, cond_set)
        return filter_table

    else:
        splitter = r' and ' if and_or_flag == 0 else r' or '
        cond_set = [u.strip(' ;')
                    for u in re.split(splitter, ' '.join(cond_list))]

        if len(cond_set) > 2:
            # print(cond_set)
            print("SQLEngineError: Command not supported by engine")
            exit()

        filter_set = []
        for condition in cond_set:
            filter_set.append(eval_cond(tables, condition))

        if len(filter_set) == 1:
            return filter_set[0]

        filter_table = Table('final', filter_set[0].cols)
        final_data = []

        for row in filter_set[0].data:
            if row in filter_set[1].data:
                final_data.append(row)

        if and_or_flag == 1:
            for row in filter_set[0].data:
                if row not in final_data:
                    final_data.append(row)

            for row in filter_set[1].data:
                if row not in final_data:
                    final_data.append(row)

        filter_table.push_data(final_data)
        # print(filter_table.cols)

        # for row in filter_table.data:
        #     print(row)
        return filter_table


def join_table(tables):
    for table in tables:
        if table not in dbase.tables.keys():
            print('SQLEngineError: Table "' + table + '" not found')
            exit()

    join_table = {}

    for table in tables:
        join_table[table] = dbase.tables[table]

    return join_table


def select_parse(tokens):
    # Check for distinct keyword
    distinct_flag = 1 if tokens[1] == 'distinct' else 0

    # Index of the word 'from'
    from_index = 3 if distinct_flag else 2

    if tokens[from_index] != 'from':
        print("SQLEngineError: Command not supported by engine")
        exit()

    from_tables = tokens[from_index+1]
    table_list = re.split(r'[\ \t,]+', from_tables)

    data_table = join_table(table_list)

    data_table_final = None

    if len(tokens) >= from_index + 3:
        if 'where' in tokens[from_index + 2]:
            data_table_final = where_filter(data_table, tokens[from_index + 2])
        else:
            print("SQLEngineError: Invalid syntax")
            exit()

    else:
        data_table_final = sing_table(data_table)

    select_clause = tokens[1 + distinct_flag]
    # whatever to be selected will be in tokens[1]
    # if we don't use the distinct keyword, else it
    # will be in tokens[2]

    display_result(select_clause, data_table_final, distinct_flag)


def parse_query(query):
    query = query.strip()

    if query[-1] != ';':
        print("SQLEngineError: Command must end with semicolon")
        exit()

    parsed_q = sqlp.parse(query[:-1])[0].tokens
    token_list = sqlp.sql.IdentifierList(parsed_q).get_identifiers()

    str_tokens = []
    for token in token_list:
        str_tokens.append(token.value.lower())

    if str_tokens[0] != 'select':
        print("SQLEngineError: Command not supported by engine")
        exit()

    select_parse(str_tokens)


if __name__ == "__main__":
    dbase.fillDB()
    # print('Note:This is emulating a case-insensitive SQL engine')
    # print('Columns with same name but different case will be treated as one')
    parse_query(sys.argv[1])
