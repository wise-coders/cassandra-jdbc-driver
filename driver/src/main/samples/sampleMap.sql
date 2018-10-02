use dragos;

CREATE TABLE foo(
  k uuid PRIMARY KEY,
  L list<int>,
  M map<text, int>,
  S set<int>
);

insert into foo(k,L,M,S) values ( b017f48f-ae67-11e1-9096-005056c00008, [2, 3], {'cod': 2, 'dog': 3}, {2, 3} );



UPDATE foo SET L = L.append(1) WHERE k = 'b017f48f-ae67-11e1-9096-005056c00008';
UPDATE foo SET L = '[2, 3]' WHERE ... ; -- do we need/want to require quoting?
UPDATE foo SET S = S.add(1) WHERE ... ;
UPDATE foo SET S = '{2, 3}' WHERE ... ; -- JSON does not define a set type or syntax.  this is Python's syntax for set literals
UPDATE foo SET S = S.discard(2) WHERE ... ;
UPDATE foo SET M = M.put('ocd', 1) WHERE ... ;
UPDATE foo SET M = '{"cod": 2, "dog": 3}' WHERE ... ; -- note double quotes forced if we require quoting the map literal
UPDATE foo SET M = M.discard('cod') WHERE ... ;