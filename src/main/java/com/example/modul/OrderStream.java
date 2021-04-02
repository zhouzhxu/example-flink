package com.example.modul;

import lombok.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/3/31 18:39 PM
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OrderStream {

    private String user;

    private String product;

    private Long rowtime;

    private Integer amount;

}
