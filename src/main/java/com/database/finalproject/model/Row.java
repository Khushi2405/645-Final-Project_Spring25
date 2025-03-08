package com.database.finalproject.model;

import static com.database.finalproject.constants.PageConstants.PADDING_BYTE;

import java.util.Arrays;

public record Row(byte[] movieId, byte[] movieTitle) {
    @Override
    public String toString() {
        return "Row{" +
                "movieId=" + new String(removeTrailingBytes(movieId)).trim() +
                ", title=" + new String(removeTrailingBytes(movieTitle)).trim() +
                '}';
    }

    public Row {
        movieId = truncateOrPadByteArray(movieId, 9);
        movieTitle = truncateOrPadByteArray(movieTitle, 30);
    }

    private static byte[] truncateOrPadByteArray(byte[] value, int maxLength) {
        if (value.length > maxLength) {
            return Arrays.copyOf(value, maxLength); // Truncate safely at byte level
        } else {
            byte[] padded = new byte[maxLength];
            System.arraycopy(value, 0, padded, 0, value.length); // Copy original bytes
            Arrays.fill(padded, value.length, maxLength, PADDING_BYTE); // Fill remaining space with 0x7F
            return padded;
        }
    }

    private static byte[] removeTrailingBytes(byte[] input) {
        int endIndex = input.length;
        for (int i = input.length - 1; i >= 0; i--) {
            if (input[i] != PADDING_BYTE) {  // Only remove custom padding byte
                endIndex = i + 1;
                break;
            }
        }
        return Arrays.copyOf(input,endIndex);
    }

//    private static byte[] convertBytesToBinary(byte[] byteArray) {
//        StringBuilder binaryString = new StringBuilder();
//
//        for (byte b : byteArray) {
//            binaryString.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
//        }
//
//        // Convert the binary string to a byte array using UTF-8 encoding
//        // Convert binary string to byte array (still preserving 8-bit grouping)
//        int length = binaryString.length() / 8;
//        byte[] result = new byte[length];
//        for (int i = 0; i < length; i++) {
//            result[i] = (byte) Integer.parseInt(binaryString.substring(i * 8, (i + 1) * 8), 2);
//        }
//
//        return result;
//    }
//
//    private static String binaryToString(byte[] binaryData) {
//        String binaryString = new String(binaryData, StandardCharsets.UTF_8); // Convert the byte array to a binary string
//
//        // Create a StringBuilder to store the decoded string
//        StringBuilder decodedString = new StringBuilder();
//
//        // Process the binary string 8 bits at a time (since each byte is 8 bits)
//        for (int i = 0; i < binaryString.length(); i += 8) {
//            String byteString = binaryString.substring(i, i + 8); // Extract 8 bits (1 byte)
//            int byteValue = Integer.parseInt(byteString, 2); // Convert the binary string to decimal (byte value)
//            decodedString.append((char) byteValue); // Append the corresponding character
//        }
//
//        // Return the decoded string
//        return decodedString.toString();
//    }

}
