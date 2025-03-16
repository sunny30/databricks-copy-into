package main.java;

import java.util.*;

public class PriorityQueueT {


    static class Person {
        String name ;
        Double amount ;
        Integer age ;
        Person(String name, Double amount, Integer age){
            this.age = age ;
            this.name = name ;
            this.amount = amount ;
        }

        @Override
        public boolean equals(Object obj) {
            if(getClass() != obj.getClass()){
                return false ;
            }else{
                Person po = (Person) obj ;
                if(po.amount.compareTo(amount) == 0 && po.name.equals(name) && po.age == age ) {
                    return true;
                }else{
                    return false ;
                }
            }
        }


        @Override
        public int hashCode() {
            return Objects.hash(age,name,amount) ;
        }

        @Override
        public String toString() {
            return age.toString()+","+name+","+amount.toString() ;
        }
    }

    static class PersonComparator implements Comparator<Person> {

        @Override
        public int compare(Person o1, Person o2) {
            if(o1.age == o2.age){
                return o1.amount<o2.amount?-1 : 1 ;
            }else{
                return o1.age-o2.age ;
            }
        }
    }

    static class MxPersonComparator implements Comparator<Person> {

        @Override
        public int compare(Person o1, Person o2) {
            if(o1.age == o2.age){
                return o1.amount>o2.amount?-1 : 1 ;
            }else{
                return o2.age-o1.age ;
            }
        }
    }
    public static void main(String[] args) {
        PriorityQueue<Person> minq;
        minq = new PriorityQueue<Person>(new PersonComparator());

        PriorityQueue<Person> mxq;
        mxq = new PriorityQueue<Person>(new MxPersonComparator());

        Person p1 = new Person("sharad", 12.5, 38) ;
        Person p2 = new Person("sunny", 4.5, 38) ;
        Person p3 = new Person("Nicky", 21.4, 42) ;

        minq.add(p1) ;
        minq.add(p2) ;
        minq.add(p3) ;

        mxq.add(p1) ;
        mxq.add(p2) ;
        mxq.add(p3) ;

        while(!minq.isEmpty()){
            Person p =  minq.poll();
            System.out.println(p.age+" "+p.amount + " "+ p.name);
        }

        System.out.println("Time for Max Queue");

        while(!mxq.isEmpty()){
            Person p =  mxq.poll();
            System.out.println(p.age+" "+p.amount + " "+ p.name);
        }
        Set<Person> set = new HashSet<>() ;
        set.add(p1) ;
        set.add(p2) ;
        set.add(p3) ;

        Person p4 = new Person("sharad", 12.5, 38) ;

        System.out.println(set.contains(p4));
       // System.out.println(set.contains(p1));
    }
}
