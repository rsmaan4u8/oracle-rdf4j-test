package org.graphifi;

import oracle.rdf4j.adapter.OraclePool;
import oracle.rdf4j.adapter.OracleRepository;
import oracle.rdf4j.adapter.OracleSailStore;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import java.io.PrintStream;
import java.util.Optional;

import static org.eclipse.rdf4j.query.parser.sparql.SPARQLUtil.encodeString;

public class PerformanceTest {

    public static String jdbcUrl = "jdbc:oracle:thin:@dbXXXXXXXX_high?TNS_ADMIN=/tmp/Wallet_DBXXXXXXXX" ;
    public static String user = "ADMIN";
    public static String password = "{UPDATE}";
    public static String model = "TEST";
    public static String networkOwner = "ADMIN";
    public static String networkName = "NN";

    public static void main(String[] args) {
        PrintStream out = new PrintStream(System.out);


        OraclePool op = null;
        OracleSailStore store = null;
        Repository sr = null;
        RepositoryConnection conn = null;
        try {
            op = new OraclePool(jdbcUrl, user, password);
            store = new OracleSailStore(op, model, networkOwner, networkName);
            sr = new OracleRepository(store);
            conn = sr.getConnection();

            Model model = new LinkedHashModel();
            ValueFactory valueFactory = sr.getValueFactory();

            int statementCount = 200;
            for(int i = 1; i<= statementCount; i++) {
                IRI subject = valueFactory.createIRI("urn:s");
                model.add(subject, valueFactory.createIRI("urn:p"+i), valueFactory.createLiteral(i), subject);
            }

            testModelAdd(conn, model);

            testSPARQLUpdate(conn, model);


        } catch (Throwable e) {
            e.printStackTrace();
        }
        finally {
            if (conn != null && conn.isOpen()) {
                // conn.clear();
                conn.close();
            }
            //  OracleUtils.dropSemanticModelAndTables(op.getOracleDB(), model, null, null, networkOwner, networkName);
            sr.shutDown();
            store.shutDown();
            op.close();
        }
    }

    private static void testModelAdd(RepositoryConnection conn, Model model) {
        System.out.println();
        System.out.println("*****************");
        System.out.println("Test model add");
        System.out.println("*****************");
        long timeBeforeAll = System.currentTimeMillis();

        int runCount = 5;
        for(int i = 1; i <= runCount; i++) {
            conn.clear();

            long start = System.currentTimeMillis();

            conn.begin();
            conn.add(model);
            conn.commit();

            System.out.println("Model length : " + model.size());
            System.out.println("Time in millis : " + (System.currentTimeMillis() - start));
        }

        System.out.println("");
        System.out.println("");
        System.out.println("Total run count  : " + runCount);
        long totalTime = System.currentTimeMillis() - timeBeforeAll;
        System.out.println("Total time for clear and add : " + totalTime);

        System.out.println("Average time for clear and add : " + (totalTime / runCount));
    }

    private static void testSPARQLUpdate(RepositoryConnection conn, Model model) {
        System.out.println();
        System.out.println("*****************");
        System.out.println("Test SPARQL update");
        System.out.println("*****************");

        StringBuilder query = new StringBuilder();
        query.append("INSERT DATA {");

        model.contexts().forEach(ct -> {
            Model graphStatements = model.filter(null, null, null, ct);
            query.append("GRAPH ");
            query.append(toStringForQuery(ct));
            query.append(" {");
            graphStatements.forEach(st -> {
                toGraphPattern(query, st);
            });
            query.append(" }");

        });
        query.append("}");

        String queryString = query.toString();

        long timeBeforeAllUpdate = System.currentTimeMillis();

        int runCount = 5;
        for(int i = 1; i <= runCount; i++) {
            conn.clear();

            long start = System.currentTimeMillis();
            conn.begin();
           // System.out.println(queryString);
            Update update = conn.prepareUpdate(QueryLanguage.SPARQL, queryString);
            update.execute();
            conn.commit();
            System.out.println("Model length : " + model.size());
            System.out.println("SPARQL Update time in millis : " + (System.currentTimeMillis() - start));
        }

        System.out.println("");
        System.out.println("");
        System.out.println("Total run count  : " + runCount);
        long totalTime = System.currentTimeMillis() - timeBeforeAllUpdate;
        System.out.println("Total time for clear and add : " + totalTime);

        System.out.println("Average time for clear and add : " + (totalTime / runCount));

    }

    private static void toGraphPattern(StringBuilder sparqlUpdate, Statement s) {
        sparqlUpdate.append("\n");

        sparqlUpdate.append(toStringForQuery(s.getSubject()));
        sparqlUpdate.append(" ");
        sparqlUpdate.append(toStringForQuery(s.getPredicate()));
        sparqlUpdate.append(" ");
        sparqlUpdate.append(toStringForQuery(s.getObject()));
        sparqlUpdate.append(". ");
        sparqlUpdate.append("\n");
    }

    public static String toStringForQuery(Value value) {
        if(value instanceof Resource) {
            if(value instanceof BNode) {
                BNode bNode = (BNode)value;
                return bNode.toString();
            } else {
                return "<"+encodeString(value.stringValue())+">";
            }
        } else if (value instanceof Literal) {
            Literal literal = (Literal) value;
            IRI dataType = literal.getDatatype();
            Optional<String> language = literal.getLanguage();
            String stringValue =  encodeString(literal.stringValue());
            StringBuilder sb = new StringBuilder('"'+stringValue+'"');

            if(language.isPresent()) {
                String lang = language.get();
                sb.append("@" + encodeString(lang));
            } else if (dataType != null) {
                sb.append("^^<" + encodeString(dataType.toString()) + ">");
            }
            return sb.toString();
        } else {
            throw new RuntimeException("Unknown type "+value);
        }

    }


}
