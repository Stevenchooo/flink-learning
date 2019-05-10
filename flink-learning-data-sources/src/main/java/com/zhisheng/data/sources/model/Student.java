package com.zhisheng.data.sources.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Administrator
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {
    public int id;
    public String batch_no;
    public String table_name;
    public int task_type;
}
