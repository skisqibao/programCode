package com.ky.repository;

import com.ky.entity.MediaNode;
import com.ky.entity.UserNode;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author xwj
 * @className UserRepository
 * @description TODO
 * @date 2020/8/31 20:04
 */
@Component
public interface UserRepository extends Neo4jRepository<MediaNode, Long> {

    @Query("MATCH (n:User) RETURN n ")
    List<UserNode> getUserNodeList();

    @Query("create (n:User{age:{age},name:{name}}) RETURN n ")
    void addUserNodeList(@Param("name") String name, @Param("age") int age);

    @Query("MERGE (m:Media {videoId: {videoId}, videoName: {videoName},videoScore:{videoScore},videoRegion:{videoRegion},videoType:{videoType}}) with m MERGE(d:Director {directName:{directName}}) MERGE (m) <-[:DIRECTED]-(d) with m MERGE(a:Actor {actorName: {actorName}}) MERGE (m)<-[:ACTED_IN]-(a) with m MERGE(t:Plot {videoPlot: {videoPlot}}) MERGE (m) -[:IN_GENRE]-> (t) with m MERGE(r:Region {videoRegion: {videoRegion}}) MERGE (m) -[:IN_REGION]-> (r)")
    void addMediaNodeList(@Param("videoId") String videoId, @Param("videoName") String videoName, @Param("videoScore") String videoScore, @Param("videoRegion") String videoRegion, @Param("videoType") String videoType
            , @Param("directName") String directName, @Param("actorName") String actorName, @Param("videoPlot") String videoPlot);

    @Query("MATCH (m:Media) WHERE m.videoId={videoId} MATCH (m)-[:IN_GENRE]->(p:Plot)<-[:IN_GENRE]-(rec:Media) WITH m, rec, COUNT(*) AS ps OPTIONAL MATCH (m)<-[:ACTED_IN]-(a:Actor)-[:ACTED_IN]->(rec) WITH m, rec, ps, COUNT(a) AS acs OPTIONAL MATCH (m)<-[:DIRECTED]-(d:Director)-[:DIRECTED]->(rec) WITH m, rec, ps, acs, COUNT(d) AS ds OPTIONAL MATCH (m)-[:IN_REGION]->(r:videoRegion)<-[:IN_REGION]-(rec:video) WITH m, rec, ps, acs, ds, COUNT(r) AS rs where rec.videoId<>{videoId} RETURN rec,(5*ps)+(4*acs)+(3*ds)+(2*rs) AS score ORDER BY score DESC limit 20")
    List<MediaNode> getRelativeVideos(@Param("videoId") String videoId);

}