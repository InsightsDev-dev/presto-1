package com.mobileum.range;

import io.airlift.slice.Slice;
/**
 * 
 * @author dilip kasana
 * @Date  13-Feb-2015
 */
public class RangeTests
{

    public static void main(String[] args)
    {
        RangeSerializer<Integer> rangeSerializer = IntegerRange.emptyRange.getSerializer();
        System.out.println();
        Range<Integer> integerRange = rangeSerializer.parse("(-2147483648,2147483647)");

        //int4range
        int4Tests();
        int4TestsFunctions_strictly_left_of();
        int4TestsFunctions_strictly_right_of();
        includes_or_contains();
        includes_or_contains_element();
        overlaps();
        adjacentTo();
        doesNotExtendToTheRightOf();
        doesNotExtendToTheLeftOf();
        union();
        intersection();
        minus();

    }

    private static void minus()
    {
        try {
            //empty
            String input1 = "(7,20)";
            String input2 = "(5,10)";
            String result = "[10,20)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.minus(intRange2));
            Assert(intRange1.minus(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input1 = "(0,20)";
            String input2 = "(5,10)";
            String result = "[10,20)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.minus(intRange2));
            Assert(intRange1.minus(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            if (e instanceof RuntimeException && e.getMessage().equals("result of range difference would not be contiguous")) {
                System.out.println(e.getMessage());
            }
            else {
                e.printStackTrace();
            }
        }
        try {
            //empty
            String input1 = "(,)";
            String input2 = "(,10)";
            String result = "[10,)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.minus(intRange2));
            Assert(intRange1.minus(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try {
            //empty
            String input1 = "(,)";
            String input2 = "(,10]";
            String result = "(10,)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.minus(intRange2));
            System.out.println(intRange1.minus(intRange2).toString() + " " + resultRange.toString());
            Assert(intRange1.minus(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void intersection()
    {
        try {
            //empty
            String input1 = "(7,20)";
            String input2 = "(5,10)";
            String result = "(7,10)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.intersection(intRange2));
            Assert(intRange1.intersection(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try {
            //empty
            String input1 = "(7,20)";
            String input2 = "(20,210)";
            String result = "empty";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.intersection(intRange2));
            Assert(intRange1.intersection(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try {
            //empty
            String input1 = "(7,20)";
            String input2 = "[20,210)";
            String result = "empty";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.intersection(intRange2));
            Assert(intRange1.intersection(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input1 = "(7,20]";
            String input2 = "[20,210)";
            String result = "[20,20]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.intersection(intRange2));
            Assert(intRange1.intersection(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input1 = "(-1000,)";
            String input2 = "(,1000)";
            String result = "(-1000,1000)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.intersection(intRange2));
            Assert(intRange1.intersection(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void union()
    {
        try {
            //empty
            String input1 = "(7,20)";
            String input2 = "(5,10)";
            String result = "(5,20)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.union(intRange2));
            Assert(intRange1.union(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        //result of range union would not be contiguous
        try {
            //empty
            String input1 = "(10,20)";
            String input2 = "(20,100)";
            String result = "(10,100)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.union(intRange2));
            Assert(intRange1.union(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            if (e instanceof RuntimeException && e.getMessage().contains("result of range union would not be contiguous"))
                System.out.println(e.getMessage());
            else {
                e.printStackTrace();
            }
        }
        try {
            //empty
            String input1 = "(10,20)";
            String input2 = "[20,100)";
            String result = "(10,100)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.union(intRange2));
            Assert(intRange1.union(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input1 = "(5,15)";
            String input2 = "[10,20]";
            String result = "(5,20]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.union(intRange2));
            Assert(intRange1.union(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input1 = "(5,)";
            String input2 = "[,10]";
            String result = "(,]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.union(intRange2));
            Assert(intRange1.union(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input1 = "empty";
            String input2 = "[,10]";
            String result = "(,10]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input1);
            Range<Integer> intRange2 = intSerializer.parse(input2);
            Range<Integer> resultRange = intSerializer.parse(result);
            System.out.println(intRange1.union(intRange2));
            Assert(intRange1.union(intRange2).toString().equals(resultRange.toString()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void doesNotExtendToTheLeftOf()
    {
        try {
            //empty
            String input = "(7,20)";
            String result = "(5,10)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.notStartsBefore(intRange2));
            System.out.println(intRange1.notStartsBefore(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try {
            //empty
            String input = "(7,20)";
            String result = "(7,10)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.notStartsBefore(intRange2));
            System.out.println(intRange1.notStartsBefore(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(7,20)";
            String result = "(8,10)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.notStartsBefore(intRange2));
            System.out.println(intRange1.notStartsBefore(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(7,20)";
            String result = "[8,10)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.notStartsBefore(intRange2));
            System.out.println(intRange1.notStartsBefore(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(-100,)";
            String result = "(-99,)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.notStartsBefore(intRange2));
            System.out.println(intRange1.notStartsBefore(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(,)";
            String result = "(,)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.notStartsBefore(intRange2));
            System.out.println(intRange1.notStartsBefore(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void doesNotExtendToTheRightOf()
    {
        try {
            //empty
            String input = "(1,20)";
            String result = "(18,20)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.notEndsAfter(intRange2));
            System.out.println(intRange1.notEndsAfter(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "empty";
            String result = "(18,20)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.notEndsAfter(intRange2));
            System.out.println(intRange1.notEndsAfter(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[,)";
            String result = "[,]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.notEndsAfter(intRange2));
            System.out.println(intRange1.notEndsAfter(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[,102)";
            String result = "[100,100]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.notEndsAfter(intRange2));
            System.out.println(intRange1.notEndsAfter(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try {
            //empty
            String input = "[,101]";
            String result = "[100,100]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.notEndsAfter(intRange2));
            System.out.println(intRange1.notEndsAfter(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try {
            //empty
            String input = "[,5]";
            String result = "[-100,2]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.notEndsAfter(intRange2));
            System.out.println(intRange1.notEndsAfter(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void adjacentTo()
    {
        try {
            //empty
            String input = "(3,7)";
            String result = "(6,12)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.isAdjacentTo(intRange2));
            System.out.println(intRange1.isAdjacentTo(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(6,12)";
            String result = "(3,7)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.isAdjacentTo(intRange2));
            System.out.println(intRange1.isAdjacentTo(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(3,6)";
            String result = "(6,10)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.isAdjacentTo(intRange2));
            System.out.println(intRange1.isAdjacentTo(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try {
            //empty
            String input = "(,4)";
            String result = "[4,)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.isAdjacentTo(intRange2));
            System.out.println(intRange1.isAdjacentTo(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(,4]";
            String result = "(4,)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.isAdjacentTo(intRange2));
            System.out.println(intRange1.isAdjacentTo(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(,5)";
            String result = "(4,)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.isAdjacentTo(intRange2));
            System.out.println(intRange1.isAdjacentTo(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(,5)";
            String result = "empty";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.isAdjacentTo(intRange2));
            System.out.println(intRange1.isAdjacentTo(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(,)";
            String result = "[,]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.isAdjacentTo(intRange2));
            System.out.println(intRange1.isAdjacentTo(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void overlaps()
    {

        try {
            //empty
            String input = "(3,7)";
            String result = "(4,12)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.overlaps(intRange2));
            System.out.println(intRange1.overlaps(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[8,12]";
            String result = "[7,9]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.overlaps(intRange2));
            System.out.println(intRange1.overlaps(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[8,12]";
            String result = "(12,13]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.overlaps(intRange2));
            System.out.println(intRange1.overlaps(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(1,2)";
            String result = "[3,14]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.overlaps(intRange2));
            System.out.println(intRange1.overlaps(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(8,10)";
            String result = "[9,9]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.overlaps(intRange2));
            System.out.println(intRange1.overlaps(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(8,10)";
            String result = "[9,10)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.overlaps(intRange2));
            System.out.println(intRange1.overlaps(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "empty";
            String result = "[,]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.overlaps(intRange2));
            System.out.println(intRange1.overlaps(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[,]";
            String result = "[100,1000]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.overlaps(intRange2));
            System.out.println(intRange1.overlaps(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[,]";
            String result = "[,]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.overlaps(intRange2));
            System.out.println(intRange1.overlaps(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void includes_or_contains_element()
    {

        try {
            //empty
            String input = "[8,12]";
            String result = "[8,9]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.includes(9));
            System.out.println(intRange1.includes(9));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[8,12]";
            String result = "[7,9]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.includes(7));
            System.out.println(intRange1.includes(7));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[8,12]";
            String result = "[8,13]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.includes(13));
            System.out.println(intRange1.includes(13));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[8,12]";
            String result = "[7,14]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.includes(14));
            System.out.println(intRange1.includes(14));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(8,10)";
            String result = "[9,9]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.includes(9));
            System.out.println(intRange1.includes(9));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(8,10)";
            String result = "[9,10)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.includes(10));
            System.out.println(!intRange1.includes(10));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "empty";
            String result = "[,]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.includes(10));
            System.out.println(intRange1.includes(10));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[,]";
            String result = "[100,1000]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.includes(100));
            System.out.println(intRange1.includes(100));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[,]";
            String result = "[,]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.includes(0));
            System.out.println(intRange1.includes(0));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void includes_or_contains()
    {
        try {
            //empty
            String input = "[8,12]";
            String result = "[8,9]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.includes(intRange2));
            System.out.println(intRange1.includes(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[8,12]";
            String result = "[7,9]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.includes(intRange2));
            System.out.println(intRange1.includes(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[8,12]";
            String result = "[8,13]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.includes(intRange2));
            System.out.println(intRange1.includes(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[8,12]";
            String result = "[7,14]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.includes(intRange2));
            System.out.println(intRange1.includes(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(8,10)";
            String result = "[9,9]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.includes(intRange2));
            System.out.println(intRange1.includes(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "(8,10)";
            String result = "[9,10)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.includes(intRange2));
            System.out.println(intRange1.includes(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "empty";
            String result = "[,]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.includes(intRange2));
            System.out.println(intRange1.includes(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[,]";
            String result = "[100,1000]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.includes(intRange2));
            System.out.println(intRange1.includes(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[,]";
            String result = "[,]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.includes(intRange2));
            System.out.println(intRange1.includes(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void int4TestsFunctions_strictly_right_of()
    {

        try {
            //empty
            String input = "[10,12]";
            String result = "[8,9]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.after(intRange2));
            System.out.println(intRange1.after(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[13,14]";
            String result = "[10,13]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.after(intRange2));
            System.out.println(intRange1.after(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[12,14]";
            String result = "[10,14]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.after(intRange2));
            System.out.println(intRange1.after(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[13,14]";
            String result = "[10,13)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.after(intRange2));
            System.out.println(intRange1.after(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String input = "(13,14]";
            String result = "[10,13]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.after(intRange2));
            System.out.println(intRange1.after(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void int4TestsFunctions_strictly_left_of()
    {
        try {
            //empty
            String input = "[10,12]";
            String result = "[13,14]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.before(intRange2));
            System.out.println(intRange1.before(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[10,13]";
            String result = "[13,14]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.before(intRange2));
            System.out.println(intRange1.before(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[10,14]";
            String result = "[12,14]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(!intRange1.before(intRange2));
            System.out.println(intRange1.before(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            //empty
            String input = "[10,13)";
            String result = "[13,14]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.before(intRange2));
            System.out.println(intRange1.before(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String input = "[10,13]";
            String result = "(13,14]";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange1 = intSerializer.parse(input);
            Range<Integer> intRange2 = intSerializer.parse(result);
            Assert(intRange1.before(intRange2));
            System.out.println(intRange1.before(intRange2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void int4Tests()
    {
        try {
            //empty
            String input = "empty";
            String result = "empty";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange = intSerializer.parse(input);
            Assert(intRange.toString().equals(result));
            Slice sss = intRange.getRangeAsSlice();
            Range<Integer> intRange2 = intSerializer.parse(sss);
            Assert(intRange2.toString().equals(result));
            System.out.println(intRange2);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String input = "(-2147483648,2147483647)";
            String result = "[-2147483647,2147483647)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange = intSerializer.parse(input);
            Assert(intRange.toString().equals(result));
            Slice sss = intRange.getRangeAsSlice();
            Range<Integer> intRange2 = intSerializer.parse(sss);
            Assert(intRange2.toString().equals(result));
            System.out.println(intRange2);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String input = "(-2147483648,2147483647)";
            String result = "[-2147483647,2147483647)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange = intSerializer.parse(input);
            Assert(intRange.toString().equals(result));
            Slice sss = intRange.getRangeAsSlice();
            Range<Integer> intRange2 = intSerializer.parse(sss);
            Assert(intRange2.toString().equals(result));
            System.out.println(intRange2);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String input = "(-2147483649,2147483647)";
            String result = null;
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange = intSerializer.parse(input);
            Assert(intRange.toString().equals(result));
            Slice sss = intRange.getRangeAsSlice();
            Range<Integer> intRange2 = intSerializer.parse(sss);
            Assert(intRange2.toString().equals(result));
            System.out.println(intRange2);
        }
        catch (NumberFormatException e) {
            // e.printStackTrace();
        }
        try {
            String input = "(-2147483648,2147483647]";
            String result = "[-2147483647,2147483647)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange = intSerializer.parse(input);
            Assert(intRange.toString().equals(result));
            Slice sss = intRange.getRangeAsSlice();
            Range<Integer> intRange2 = intSerializer.parse(sss);
            Assert(intRange2.toString().equals(result));
            System.out.println(intRange2);
        }
        catch (RuntimeException e) {
            if (!e.getMessage().contains("integer out of range")) {
                e.printStackTrace();
            }
        }
        try {
            String input = "(10,10]";
            String result = "empty";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange = intSerializer.parse(input);
            Assert(intRange.toString().equals(result));
            Slice sss = intRange.getRangeAsSlice();
            Range<Integer> intRange2 = intSerializer.parse(sss);
            Assert(intRange2.toString().equals(result));
            System.out.println(intRange2);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String input = "[10,10]";
            String result = "[10,11)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange = intSerializer.parse(input);
            Assert(intRange.toString().equals(result));
            Slice sss = intRange.getRangeAsSlice();
            Range<Integer> intRange2 = intSerializer.parse(sss);
            Assert(intRange2.toString().equals(result));
            System.out.println(intRange2);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String input = "(9,11]";
            String result = "[10,12)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange = intSerializer.parse(input);
            Assert(intRange.toString().equals(result));
            Slice sss = intRange.getRangeAsSlice();
            Range<Integer> intRange2 = intSerializer.parse(sss);
            Assert(intRange2.toString().equals(result));
            System.out.println(intRange2);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String input = "[10,11)";
            String result = "[10,11)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange = intSerializer.parse(input);
            Assert(intRange.toString().equals(result));
            Slice sss = intRange.getRangeAsSlice();
            Range<Integer> intRange2 = intSerializer.parse(sss);
            Assert(intRange2.toString().equals(result));
            System.out.println(intRange2);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String input = "(9,11)";
            String result = "[10,11)";
            RangeSerializer<Integer> intSerializer = IntegerRange.emptyRange.getSerializer();
            Range<Integer> intRange = intSerializer.parse(input);
            Assert(intRange.toString().equals(result));
            Slice sss = intRange.getRangeAsSlice();
            Range<Integer> intRange2 = intSerializer.parse(sss);
            Assert(intRange2.toString().equals(result));
            System.out.println(intRange2);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void Assert(boolean flag)
    {
        if (!flag) {
            throw new RuntimeException("ERROR");
        }
    }
}
