package io.github.fbiville.trainings.neo4j._4_cypher_writing;

import io.github.fbiville.trainings.neo4j.internal.DoctorWhoGraph;
import io.github.fbiville.trainings.neo4j.internal.GraphTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class _3_CypherMergeTest extends GraphTests {

    @Before
    public void prepare() {
        DoctorWhoGraph.create(graphDb);
    }

    @Test
    public void should_bring_actors_Karen_Gillan_and_Caitlin_Blackwood_into_the_Amy_Pond_subgraph() {
        // Hint: Amy Pond is definitely in the graph, and the actors may be too. How can MERGE help?
        try (Transaction transaction = graphDb.beginTx()) {
            String cql =
                    "MATCH (amy:Character {character: 'Amy Pond'}) " +
                    "MERGE (amy)<-[:PLAYED]-(:Actor {actor: 'Karen Gillan'}) " +
                    "MERGE (amy)<-[:PLAYED]-(:Actor {actor: 'Caitlin Blackwood'})";

            graphDb.execute(cql);
            transaction.success();
        }

        try (Transaction ignored = graphDb.beginTx()) {
            ResourceIterator<String> actors = graphDb.execute("MATCH (:Character {character: 'Amy Pond'})<-[:PLAYED]-(a:Actor) " +
                "RETURN a.actor AS actor").columnAs("actor");
            assertThat(actors).containsOnly("Karen Gillan", "Caitlin Blackwood");
        }
    }

    @Test
    public void should_make_sure_Amy_Pond_and_Rory_Williams_are_in_love() {
        // Hint: love must go both ways ;-D
        try (Transaction transaction = graphDb.beginTx()) {
            String cql =
                "MERGE (amy:Character {character: 'Amy Pond'}) " +
                "MERGE (rory:Character {character: 'Rory Williams'}) " +
                "MERGE (amy)<-[:LOVES]-(rory) " +
                "MERGE (amy)-[:LOVES]->(rory)";

            graphDb.execute(cql);
            transaction.success();
        }

        try (Transaction ignored = graphDb.beginTx()) {
            Result result = graphDb.execute("MATCH (:Character {character: 'Amy Pond'})-[loves:LOVES]->(:Character {character: 'Rory Williams'}) RETURN loves");
            assertThat(result).hasSize(1);
            result = graphDb.execute("MATCH (:Character {character: 'Amy Pond'})<-[loves:LOVES]-(:Character {character: 'Rory Williams'}) RETURN loves");
            assertThat(result).hasSize(1);
        }
    }

    @Test
    public void should_demarcate_years_when_Amy_Pond_was_a_companion_of_the_doctor() {
        // Hint: you should set the dates only if it matches
        try (Transaction transaction = graphDb.beginTx()) {
            String cql =
                    "MERGE (:Character {character: 'Amy Pond'})-[c:COMPANION_OF]->(:Character {character: 'Doctor'})" +
                    "ON MATCH SET c.start = 2010, c.end = 2013";

            graphDb.execute(cql);
            transaction.success();
        }

        try (Transaction ignored = graphDb.beginTx()) {
            Result result = graphDb.execute("MATCH (:Character {character: 'Amy Pond'})-[c:COMPANION_OF {`start`:2010, `end`:2013}]->(:Character {character: 'Doctor'}) RETURN c");
            assertThat(result).hasSize(1);
        }
    }
}
