Storm-filterkey-bolt
======================
This bolt operates filtering data from a stream of tuples according a mapped list of crterias specified in Topology's config.
The tuple contains a JSON object with two fileds: "extraData" and "message". The value of the second one will never be modified,
but will be propagated to next bolt. The value of the first one, this is, "extraData", contains another Json composed of pairs of keys
and values intendeded to be a tag or header for the message.

The main goal of this bolt is to reduce the number of the pairs in the "extraData". To acomplish the goal a list of criterias must be
supplied int Topology's file config, as a list of mapped criterias.

### Example: ######
the tuple recevied may contains this data:

> {
> "extraData":{"Name": "Peter", "Age":"33", "City": "London", "Country": "UK"},
> "message": "the original body string"
> } 

Just need this information:

```ini
 key.selection.criteria.1 = {"key":{"Name":"Peter"},"values":["City"]}" );
```
the propagated information to next bolt into topology is:

> {
> "extraData":{"Name": "Peter","City": "London"}
> "message": "the original body string"
> } 

### Version history ######
0.0.1-Snapshot

### License ######

Apache License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0

www.keedio.org
Luis Lázaro <lalazaro@keedio.com>