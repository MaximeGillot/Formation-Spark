package fr.example.formation.dataframe.dataset;

import java.io.Serializable;

// Java bean Client
public class Client implements Serializable {
    private String clientId;
    private String name;
    private int age;

    // Constructeur
    public Client(String clientId, String name, int age) {
        this.clientId = clientId;
        this.name = name;
        this.age = age;
    }

    public Client() {
    }

    // Getters et setters
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }


    public boolean isLegalAge() {
        return age >= 18;
    }
}