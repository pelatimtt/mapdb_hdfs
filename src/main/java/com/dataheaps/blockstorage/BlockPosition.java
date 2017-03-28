package com.dataheaps.blockstorage;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by admin on 20/1/17.
 */

@Data @AllArgsConstructor
public class BlockPosition {
    long block;
    int offset;
}
