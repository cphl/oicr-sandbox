#!/bin/bash 
echo "Generating list of keys"
for reg in `cat regions`; do mkdir -p $reg; echo $reg >> $reg/keys; echo "-----------------">> $reg/keys;echo "" >> $reg/keys; aws --region $reg ec2 describe-key-pairs >>$reg/keys;done

echo "Generating list of instances"
for reg in `cat regions`; do echo $reg >> $reg/instances; echo "-----------------">> $reg/instances;aws --region $reg ec2 describe-instances >>$reg/all_instances;done


echo "Generating wide_open_security_rules"
for reg in `cat regions`; do echo $reg >> $reg/sec_rules;date >> $reg/sec_rules; echo "-----------------">> $reg/sec_rules;aws --output json --region $reg ec2 describe-security-groups | perl security_rules.pl | grep -B2 "from source 0.0.0.0/0" > $reg/sec_rules;done
