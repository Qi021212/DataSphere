def format_output(rows, columns=None):
    """格式化查询结果输出，使用表格边框"""
    if not rows:
        return "没有结果"

    if columns is None:
        if not rows:
            return "没有结果"
        columns = list(rows[0].keys())

    if not columns:
        return "没有列可显示"

    # 计算每列的最大宽度（包括列名和所有数据）
    col_widths = []
    for col in columns:
        max_width = len(str(col))  # 从列名开始
        for row in rows:
            value = str(row.get(col, 'NULL'))  # 使用 .get 避免 KeyError，空值显示为 'NULL'
            max_width = max(max_width, len(value))
        col_widths.append(max_width)

    # 构建分隔行 (例如: "+-------+----------+--------+")
    separator = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"

    # 构建格式化字符串 (例如: "| {:<7} | {:<10} | {:<8} |")
    # {:<7} 表示左对齐，宽度为7
    format_str = "| " + " | ".join([f"{{:<{w}}}" for w in col_widths]) + " |"

    output_lines = []
    # 添加顶部边框
    output_lines.append(separator)
    # 添加表头
    header_values = [str(col) for col in columns]
    output_lines.append(format_str.format(*header_values))
    # 添加表头下方的分隔线
    output_lines.append(separator)
    # 添加数据行
    for row in rows:
        values = [str(row.get(col, 'NULL')) for col in columns]
        output_lines.append(format_str.format(*values))
    # 添加底部边框
    output_lines.append(separator)

    # 返回总行数信息
    result = "\n".join(output_lines)
    result += f"\n\n{len(rows)} row(s) returned"
    return result