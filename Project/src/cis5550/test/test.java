package cis5550.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.*;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

public class test {

	public static void main(String[] args) throws Exception {
		String html = "<h1>apparently "
				+ "trying "
				+ "to</h1>recollect \r\n"
				+ "something. \r\n"
				+ "Prince \r\n"
				+ "Andrew \r\n"
				+ "stepped \r\n"
				+ "forward \r\n"
				+ "from \r\n"
				+ "<h1>among</h1> \r\n"
				+ "the\r\n"
				+ "suite \r\n"
				+ "and \r\n"
				+ "said \r\n"
				+ "in \r\n"
				+ "French:\r\n"
				+ "\r\n"
				+ "\"You \r\n"
				+ "told \r\n"
				+ "me \r\n"
				+ "to \r\n"
				+ "remind \r\n"
				+ "you \r\n"
				+ "of \r\n"
				+ "the \r\n"
				+ "officer \r\n"
				+ "Dolokhov, \r\n"
				+ "<h2>reduced</h2> \r\n"
				+ "to \r\n"
				+ "the\r\n"
				+ "ranks<p>"
				+ "in"
				+ "</p>this \r\n"
				+ "regiment.\"\r\n"
				+ "\r\n"
				+ "\"Where \r\n"
				+ "is \r\n"
				+ "Dolokhov?\" \r\n"
				+ "asked \r\n"
				+ "Kutuzov.\r\n"
				+ "\r\n"
				+ "Dolokhov, </p>\r\n"
				+ "who \r\n"
				+ "had \r\n"
				+ "<title>already</title> \r\n"
				+ "changed \r\n"
				+ "into \r\n"
				+ "a \r\n"
				+ "soldier's \r\n"
				+ "gray \r\n"
				+ "greatcoat,\r\n"
				+ "did<p>\r\n"
				+ "not \r\n"
				+ "wait \r\n"
				+ "to \r\n"
				+ "be \r\n"
				+ "called. \r\n"
				+ "The \r\n"
				+ "shapely \r\n"
				+ "figure \r\n"
				+ "<a href=\"/CN9CmwX.html\">of</a> \r\n"
				+ "the \r\n"
				+ "fair-haired\r\n"
				+ "soldier,<p>\r\n"
				+ "with \r\n"
				+ "his \r\n"
				+ "clear \r\n"
				+ "blue \r\n"
				+ "eyes, \r\n"
				+ "stepped \r\n"
				+ "forward \r\n"
				+ "from \r\n"
				+ "the \r\n"
				+ "<a href=\"/CN9CmwX.html\">ranks,</a> \r\n"
				+ "went\r\n"
				+ "up \r\n"
				+ "to \r\n"
				+ "the \r\n"
				+ "commander \r\n"
				+ "in \r\n"
				+ "chief, \r\n"
				+ "and \r\n"
				+ "presented \r\n"
				+ "arms.\r\n"
				+ "\r\n"
				+ "\"Have \r\n"
				+ "you \r\n"
				+ "a \r\n"
				+ "complaint \r\n"
				+ "to \r\n"
				+ "make?\" \r\n"
				+ "Kutuzov \r\n"
				+ "asked \r\n"
				+ "with \r\n"
				+ "a \r\n"
				+ "slight \r\n"
				+ "frown.\r\n"
				+ "\r\n"
				+ "\"This \r\n"
				+ "is \r\n"
				+ "Dolokhov,\" \r\n"
				+ "said \r\n"
				+ "Prince \r\n"
				+ "Andrew.\r\n"
				+ "\r\n"
				+ "\"Ah!\" \r\n"
				+ "said \r\n"
				+ "Kutuzov. \r\n"
				+ "\"I \r\n"
				+ "hope \r\n"
				+ "this \r\n"
				+ "will \r\n"
				+ "be \r\n"
				+ "a \r\n"
				+ "lesson \r\n"
				+ "to \r\n"
				+ "you. \r\n"
				+ "Do \r\n"
				+ "your\r\n"
				+ "duty. \r\n"
				+ "The \r\n"
				+ "Emperor \r\n"
				+ "is \r\n"
				+ "gracious, \r\n"
				+ "and \r\n"
				+ "I \r\n"
				+ "shan't \r\n"
				+ "forget \r\n"
				+ "you \r\n"
				+ "if \r\n"
				+ "you \r\n"
				+ "deserve\r\n"
				+ "well.\"\r\n"
				+ "\r\n"
				+ "The \r\n"
				+ "clear \r\n"
				+ "blue \r\n"
				+ "eyes \r\n"
				+ "looked \r\n"
				+ "at \r\n"
				+ "the \r\n"
				+ "commander \r\n"
				+ "in \r\n"
				+ "chief \r\n"
				+ "just \r\n"
				+ "as \r\n"
				+ "boldly \r\n"
				+ "as\r\n"
				+ "they \r\n"
				+ "had \r\n"
				+ "looked \r\n"
				+ "at \r\n"
				+ "the \r\n"
				+ "regimental \r\n"
				+ "commander, \r\n"
				+ "seeming \r\n"
				+ "by \r\n"
				+ "their \r\n"
				+ "expression\r\n"
				+ "to \r\n"
				+ "tear \r\n"
				+ "open \r\n"
				+ "the \r\n"
				+ "veil \r\n"
				+ "of \r\n"
				+ "convention \r\n"
				+ "that \r\n"
				+ "separates \r\n"
				+ "a \r\n"
				+ "commander \r\n"
				+ "in \r\n"
				+ "chief\r\n"
				+ "so \r\n"
				+ "widely \r\n"
				+ "from \r\n"
				+ "a \r\n"
				+ "private.\r\n"
				+ "\r\n"
				+ "\"One \r\n"
				+ "thing \r\n"
				+ "I \r\n"
				+ "ask \r\n"
				+ "of \r\n"
				+ "your \r\n"
				+ "excellency,\" \r\n"
				+ "Dolokhov \r\n"
				+ "said \r\n"
				+ "in \r\n"
				+ "his \r\n"
				+ "firm,\r\n"
				+ "ringing, \r\n"
				+ "deliberate \r\n"
				+ "voice. \r\n"
				+ "\"I \r\n"
				+ "ask \r\n"
				+ "an \r\n"
				+ "opportunity \r\n"
				+ "to \r\n"
				+ "atone \r\n"
				+ "for \r\n"
				+ "my \r\n"
				+ "fault\r\n"
				+ "and \r\n"
				+ "prove \r\n"
				+ "my \r\n"
				+ "devotion \r\n"
				+ "to \r\n"
				+ "His \r\n"
				+ "Majesty \r\n"
				+ "the \r\n"
				+ "Emperor \r\n"
				+ "and \r\n"
				+ "to \r\n"
				+ "<a href=\"/ND3.html\">Russia!\"\r\n"
				+ "\r\n"
				+ "Kutuzov</a> \r\n"
				+ "turned \r\n"
				+ "away. \r\n"
				+ "The \r\n"
				+ "same \r\n"
				+ "smile \r\n"
				+ "of \r\n"
				+ "the \r\n"
				+ "eyes \r\n"
				+ "with \r\n"
				+ "which \r\n"
				+ "he \r\n"
				+ "had\r\n"
				+ "turned \r\n"
				+ "from \r\n"
				+ "Captain \r\n"
				+ "Timokhin \r\n"
				+ "again \r\n"
				+ "flitted \r\n"
				+ "over \r\n"
				+ "his \r\n"
				+ "face. \r\n"
				+ "He \r\n"
				+ "turned\r\n"
				+ "away \r\n"
				+ "with \r\n"
				+ "a \r\n"
				+ "grimace<p>\r\n"
				+ "as \r\n"
				+ "if \r\n"
				+ "to \r\n"
				+ "say \r\n"
				+ "that \r\n"
				+ "everything \r\n"
				+ "Dolokhov \r\n"
				+ "had \r\n"
				+ "said \r\n"
				+ "to\r\n"
				+ "him \r\n"
				+ "and \r\n"
				+ "everything \r\n"
				+ "he \r\n"
				+ "could \r\n"
				+ "say \r\n"
				+ "had \r\n"
				+ "long \r\n"
				+ "been \r\n"
				+ "known \r\n"
				+ "to \r\n"
				+ "him, \r\n"
				+ "that \r\n"
				+ "he \r\n"
				+ "was\r\n"
				+ "weary \r\n"
				+ "of \r\n"
				+ "it \r\n"
				+ "and \r\n"
				+ "it \r\n"
				+ "was \r\n"
				+ "not \r\n"
				+ "at \r\n"
				+ "all \r\n"
				+ "what \r\n"
				+ "he \r\n"
				+ "wanted. \r\n"
				+ "He \r\n"
				+ "turned \r\n"
				+ "away \r\n"
				+ "and\r\n"
				+ "went \r\n"
				+ "to \r\n"
				+ "the \r\n"
				+ "carriage.\r\n"
				+ "\r\n"
				+ "The \r\n"
				+ "regiment \r\n"
				+ "broke \r\n"
				+ "up \r\n"
				+ "into \r\n"
				+ "companies, \r\n"
				+ "which \r\n"
				+ "went \r\n"
				+ "to \r\n"
				+ "their \r\n"
				+ "appointed\r\n"
				+ "quarters \r\n"
				+ "near \r\n"
				+ "Braunau, \r\n"
				+ "where \r\n"
				+ "they \r\n"
				+ "hoped \r\n"
				+ "to \r\n"
				+ "receive \r\n"
				+ "boots \r\n"
				+ "and \r\n"
				+ "clothes \r\n"
				+ "and\r\n"
				+ "to \r\n"
				+ "rest \r\n"
				+ "after \r\n"
				+ "their \r\n"
				+ "hard \r\n"
				+ "marches.\r\n"
				+ "\r\n"
				+ "\"You \r\n"
				+ "won't \r\n"
				+ "bear \r\n"
				+ "me \r\n"
				+ "a \r\n"
				+ "grudge, \r\n"
				+ "Prokhor \r\n"
				+ "Ignatych?\" \r\n"
				+ "said \r\n"
				+ "the\r\n"
				+ "regimental \r\n"
				+ "commander, \r\n"
				+ "overtaking ";
		// Map<String, Integer> map = new HashMap<>();
		// map.put("title", 50);
		// map.put("h1", 30);
		// map.put("h2", 20);
		// map.put("h3", 10);
		// map.put("h4", 5);
		// map.put("h5", 3);

		// for (String tag : map.keySet()) {
		// int tagVal = map.get(tag);
		// Pattern p = Pattern.compile("<" + tag + ">(.*?)</" + tag + ">");
		// Matcher m = p.matcher(html);
		// if (m.find()) {
		// String title = m.group(1);
		// System.out.println(title);

		// String tagsRemoved = title.replaceAll("<.*?>", " ");
		// System.out.println(tagsRemoved);

		// String titleRemoved = tagsRemoved.replaceAll("[\t\r\n.,:;!?’'\"()-]", " ");
		// System.out.println("tag: " + tag + " content: " + titleRemoved);

		// System.out.println();
		// }
		// }

		Pattern p = Pattern.compile("<p[^>]*>(.*?)</p>");
		Matcher m = p.matcher(html);

		if (m.find()) {

			System.out.println("matched");

			String paragraph = m.group(1);
			String tagsRemoved = paragraph.replaceAll("<.*?>", " ");
			String firstSentence = tagsRemoved.split("\\.")[0];

			System.out.println(firstSentence);
		}

		System.out.println("fuck you");
	}
}