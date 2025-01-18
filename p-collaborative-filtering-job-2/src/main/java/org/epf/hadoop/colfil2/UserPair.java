package org.epf.hadoop.colfil2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class UserPair implements WritableComparable<UserPair> {

    private String user1;
    private String user2;

    public UserPair() {
    }

    public UserPair(String user1, String user2) {
        this.set(user1, user2);
    }

    public void set(String user1, String user2) {
        if (user1.compareTo(user2) < 0) {
            this.user1 = user1;
            this.user2 = user2;
        } else {
            this.user1 = user2;
            this.user2 = user1;
        }
    }

    public String getUser1() {
        return user1;
    }

    public String getUser2() {
        return user2;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(user1);
        out.writeUTF(user2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        user1 = in.readUTF();
        user2 = in.readUTF();
    }

    @Override
    public int compareTo(UserPair other) {
        int cmp = this.user1.compareTo(other.user1);
        if (cmp != 0) {
            return cmp;
        }
        return this.user2.compareTo(other.user2);
    }

    @Override
    public int hashCode() {
        return user1.hashCode() * 163 + user2.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        UserPair other = (UserPair) obj;
        return user1.equals(other.user1) && user2.equals(other.user2);
    }

    @Override
    public String toString() {
        return user1 + "\t" + user2;
    }
}
