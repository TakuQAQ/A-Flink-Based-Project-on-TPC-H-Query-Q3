package com.taku.tpch.model;

public class MyEvent {
    // Customer object
    public static class Customer {
        public int custKey;
        public String mktSegment;
        public Customer() {}
        public Customer(int custKey, String mktSegment) {
            this.custKey = custKey;
            this.mktSegment = mktSegment;
        }

        @Override
        public String toString() {
            return "Customer{" +
                    "custKey=" + custKey +
                    ", mktSegment='" + mktSegment + '\'' +
                    '}';
        }
    }


    // Orders object
    public static class Order {
        public int orderKey;
        public int custKey;
        public String orderDate;
        public int shipPriority;
        public Order() {}
        public Order(int orderKey, int custKey, String orderDate, int shipPriority) {
            this.orderKey = orderKey;
            this.custKey = custKey;
            this.orderDate = orderDate;
            this.shipPriority = shipPriority;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "orderKey=" + orderKey +
                    ", custKey=" + custKey +
                    ", orderDate='" + orderDate + '\'' +
                    ", shipPriority=" + shipPriority +
                    '}';
        }
    }


    // LineItem object
    public static class LineItem {
        public int orderKey;
        public double extendedPrice;
        public double discount;
        public String shipDate;
        public LineItem() {}
        public LineItem(int orderKey, double extendedPrice, double discount, String shipDate) {
            this.orderKey = orderKey;
            this.extendedPrice = extendedPrice;
            this.discount = discount;
            this.shipDate = shipDate;
        }

        @Override
        public String toString() {
            return "Lineitem{" +
                    "orderKey=" + orderKey +
                    ", extendedPrice=" + extendedPrice +
                    ", discount=" + discount +
                    ", shipDate='" + shipDate + '\'' +
                    '}';
        }
    }


    // Join 1 object
    public static class FilteredOrder {
        public int orderKey;
        public String orderDate;
        public int shipPriority;

        public FilteredOrder() {
        }

        public FilteredOrder(int orderKey, String orderDate, int shipPriority) {
            this.orderKey = orderKey;
            this.orderDate = orderDate;
            this.shipPriority = shipPriority;
        }

        @Override
        public String toString() {
            return "FilteredOrder{" +
                    "orderKey=" + orderKey +
                    ", orderDate='" + orderDate + '\'' +
                    ", shipPriority=" + shipPriority +
                    '}';
        }
    }


    // Join 2 object
    public static class Q3Result {
        public int orderKey;
        public double revenue;
        public String orderDate;
        public int shipPriority;

        public Q3Result() {}
        public Q3Result(int orderKey, double revenue, String orderDate, int shipPriority) {
            this.orderKey = orderKey;
            this.revenue = revenue;
            this.orderDate = orderDate;
            this.shipPriority = shipPriority;
        }

        @Override
        public String toString() {
            return "Q3Result{" +
                    "orderKey=" + orderKey +
                    ", revenue=" + revenue +
                    ", orderDate='" + orderDate + '\'' +
                    ", shipPriority=" + shipPriority +
                    '}';
        }
    }


    // Union of order amd lineitem tables
    public static class BusinessEvent {
        public int orderKey;
        public double revenueUnshipped;
        public String date;   // orderDate or shipDate
        public boolean isOrder;

        public BusinessEvent(int orderKey, double revenueUnshipped, String date, boolean isOrder) {
            this.orderKey = orderKey;
            this.revenueUnshipped = revenueUnshipped;
            this.date = date;
            this.isOrder = isOrder;
        }

        @Override
        public String toString() {
            return "BusinessEvent{" +
                    "orderKey=" + orderKey +
                    ", revenueUnshipped=" + revenueUnshipped +
                    ", date='" + date + '\'' +
                    ", isOrder=" + isOrder +
                    '}';
        }
    }


    // Merged business event
    public static class MergedBusinessEvent {
        public int orderKey;
        public int custKey;
        public double revenueUnshipped;
        public String date;   // orderDate or shipDate
        public boolean isOrder;

        public MergedBusinessEvent(int orderKey, int custKey, double revenueUnshipped, String date, boolean isOrder) {
            this.orderKey = orderKey;
            this.custKey = custKey;
            this.revenueUnshipped = revenueUnshipped;
            this.date = date;
            this.isOrder = isOrder;
        }

        @Override
        public String toString() {
            return "BusinessEvent{" +
                    "orderKey=" + orderKey +
                    "custKey=" + custKey +
                    ", revenueUnshipped=" + revenueUnshipped +
                    ", date='" + date + '\'' +
                    ", isOrder=" + isOrder +
                    '}';
        }
    }
}