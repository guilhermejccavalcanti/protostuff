// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from group.proto

package io.protostuff;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

public final class Club implements Externalizable, Message<Club>
{

    public static Schema<Club> getSchema()
    {
        return SCHEMA;
    }

    public static Club getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final Club DEFAULT_INSTANCE = new Club();

    // non-private fields
    // see http://developer.android.com/guide/practices/design/performance.html#package_inner
    String name;
    List<Student> student;
    List<Club> partnerClub;

    public Club()
    {

    }

    // getters and setters

    // name

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    // student

    public List<Student> getStudentList()
    {
        return student;
    }

    public void setStudentList(List<Student> student)
    {
        this.student = student;
    }

    public Student getStudent(int index)
    {
        return student == null ? null : student.get(index);
    }

    public int getStudentCount()
    {
        return student == null ? 0 : student.size();
    }

    public void addStudent(Student student)
    {
        if (this.student == null)
            this.student = new ArrayList<>();
        this.student.add(student);
    }

    // partnerClub

    public List<Club> getPartnerClubList()
    {
        return partnerClub;
    }

    public void setPartnerClubList(List<Club> partnerClub)
    {
        this.partnerClub = partnerClub;
    }

    public Club getPartnerClub(int index)
    {
        return partnerClub == null ? null : partnerClub.get(index);
    }

    public int getPartnerClubCount()
    {
        return partnerClub == null ? 0 : partnerClub.size();
    }

    public void addPartnerClub(Club partnerClub)
    {
        if (this.partnerClub == null)
            this.partnerClub = new ArrayList<>();
        this.partnerClub.add(partnerClub);
    }

    // java serialization

    @Override
    public void readExternal(ObjectInput in) throws IOException
    {
        GraphIOUtil.mergeDelimitedFrom(in, this, SCHEMA);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        GraphIOUtil.writeDelimitedTo(out, this, SCHEMA);
    }

    // message method

    @Override
    public Schema<Club> cachedSchema()
    {
        return SCHEMA;
    }

    static final Schema<Club> SCHEMA = new Schema<Club>()
    {
        // schema methods

        @Override
        public Club newMessage()
        {
            return new Club();
        }

        @Override
        public Class<Club> typeClass()
        {
            return Club.class;
        }

        @Override
        public String messageName()
        {
            return Club.class.getSimpleName();
        }

        @Override
        public String messageFullName()
        {
            return Club.class.getName();
        }

        @Override
        public boolean isInitialized(Club message)
        {
            return true;
        }

        @Override
        public void mergeFrom(Input input, Club message) throws IOException
        {
            for (int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
            {
                switch (number)
                {
                    case 0:
                        return;
                    case 1:
                        message.name = input.readString();
                        break;
                    case 2:
                        if (message.student == null)
                            message.student = new ArrayList<>();
                        message.student.add(input.mergeObject(null, Student.getSchema()));
                        break;

                    case 3:
                        if (message.partnerClub == null)
                            message.partnerClub = new ArrayList<>();
                        message.partnerClub.add(input.mergeObject(null, Club.getSchema()));
                        break;

                    default:
                        input.handleUnknownField(number, this);
                }
            }
        }

        @Override
        public void writeTo(Output output, Club message) throws IOException
        {
            if (message.name != null)
                output.writeString(1, message.name, false);

            if (message.student != null)
            {
                for (Student student : message.student)
                {
                    if (student != null)
                        output.writeObject(2, student, Student.getSchema(), true);
                }
            }

            if (message.partnerClub != null)
            {
                for (Club partnerClub : message.partnerClub)
                {
                    if (partnerClub != null)
                        output.writeObject(3, partnerClub, Club.getSchema(), true);
                }
            }

        }

        @Override
        public String getFieldName(int number)
        {
            switch (number)
            {
                case 1:
                    return "name";
                case 2:
                    return "student";
                case 3:
                    return "partnerClub";
                default:
                    return null;
            }
        }

        @Override
        public int getFieldNumber(String name)
        {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }

        final java.util.HashMap<String, Integer> fieldMap = new java.util.HashMap<>();

        {
            fieldMap.put("name", 1);
            fieldMap.put("student", 2);
            fieldMap.put("partnerClub", 3);
        }
    };

}
