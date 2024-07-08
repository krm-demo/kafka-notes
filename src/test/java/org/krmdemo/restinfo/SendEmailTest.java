package org.krmdemo.restinfo;


import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import java.util.*;

import static java.lang.String.format;

public class SendEmailTest {

    @Test
    @Tag("gmail")
    void testSendGmailSimple() {
        gmailSender().send(simpleMailMessage());
    }

    @Test
    @Tag("aws-ses")
    void testSendAwsSesSimple() {
        awsSesSender().send(simpleMailMessage());
    }

    private SimpleMailMessage simpleMailMessage() {
        SimpleMailMessage simpleMail = new SimpleMailMessage();
        simpleMail.setFrom(format("%s@gmail.com", getClass().getName()));
        simpleMail.setTo("aleksey.kurmanov.aws@gmail.com");
        simpleMail.setSubject("testSendGmailSimple()");
        simpleMail.setText("""
            "Captain Laptev" books:
            1) Смерть со школьной скамьи
            2) Лагерь обреченных
            3) Кочевая кровь
            4) Письмо ни от кого
            5) Скелет в семейном альбоме
            6) Зло из телевизора
            7) Пуля без комментариев (ещё не скачано)
            8) Портрет обнаженной  (ещё не скачано)
            9) Афера для своих  (ещё не скачано)
            """);
        return simpleMail;
    }

    private static JavaMailSender gmailSender() {
        JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
        mailSender.setHost("smtp.gmail.com");
        mailSender.setPort(587);

        mailSender.setUsername("aleksey.kurmanov.aws@gmail.com");
        mailSender.setPassword("1qaz@WSX0okm(IJN");

        Properties props = mailSender.getJavaMailProperties();
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.debug", "true");

        return mailSender;
    }

    private static JavaMailSender awsSesSender() {
        JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
        mailSender.setHost("email-smtp.us-east-2.amazonaws.com");
        mailSender.setPort(25);

        mailSender.setUsername("AKIAYFBG5OZYNIZTJ2TM");
        mailSender.setPassword("BAbieuZ0y+/udOJ/HJWRKcf3EhZfduoso6qQdHjlFzKG");

        Properties props = mailSender.getJavaMailProperties();
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.smtp.port", "25");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.starttls.required", "true");
        props.put("mail.debug", "true");

        return mailSender;
    }
}
